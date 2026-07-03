import Foundation
import SwiftProtobufPluginLibrary

@main
struct ActrFrameworkGenerator {
  static let version = "0.4.10"

  struct RemoteServiceInfo {
    let serviceName: String
    let routeKeys: [String]
    let fileName: String
  }

  struct MethodMetadata: Encodable {
    let name: String
    let snake_name: String
    let input_type: String
    let output_type: String
    let route_key: String
  }

  struct LocalServiceMetadata: Encodable {
    let name: String
    let package: String
    let proto_file: String
    let handler_interface: String
    let workload_type: String
    let dispatcher_type: String
    let methods: [MethodMetadata]
  }

  struct RemoteServiceMetadata: Encodable {
    let name: String
    let package: String
    let proto_file: String
    let actr_type: String
    let client_type: String
    let methods: [MethodMetadata]
  }

  struct ActrGenMetadata: Encodable {
    let plugin_version: String
    let language: String
    let local_services: [LocalServiceMetadata]
    let remote_services: [RemoteServiceMetadata]
  }

  static func main() throws {
    // Handle command line arguments
    if CommandLine.arguments.contains("--version") || CommandLine.arguments.contains("-v") {
      print(version)
      return
    }

    // Read the request from stdin
    let requestData = FileHandle.standardInput.readDataToEndOfFile()
    let request = try Google_Protobuf_Compiler_CodeGeneratorRequest(serializedBytes: requestData)

    // Parse parameters
    let parameters = request.parameter.split(separator: ",").reduce(into: [String: String]()) {
      dict, pair in
      let parts = pair.split(separator: "=", maxSplits: 1)
      if parts.count == 2 {
        dict[String(parts[0])] = String(parts[1])
      } else {
        dict[String(parts[0])] = ""
      }
    }
    let visibility = parameters["Visibility"] ?? "Internal"
    let isPublic = visibility.lowercased() == "public"
    let accessModifier = isPublic ? "public " : ""
    let manufacturer = parameters["Manufacturer"] ?? "acme"

    let localFileParam = parameters["LocalFile"]
    let localFilesParam = Set(
      (parameters["LocalFiles"] ?? "").split(separator: ":").map(String.init))
    let protoSourceParam = parameters["ProtoSource"]?.lowercased()
    let globalProtoSource =
      protoSourceParam ?? ((localFileParam != nil || !localFilesParam.isEmpty) ? "remote" : "local")

    let remoteFilesParam = Set(
      (parameters["RemoteFiles"] ?? "").split(separator: ":").map(String.init))

    // Parse RemoteFileActrTypes parameter: file1=actr_type1;file2=actr_type2
    // The top-level protoc parameter string is comma-separated, so mappings
    // inside one parameter use semicolons.
    var remoteFileToActrType: [String: String] = [:]
    if let remoteFileActrTypesParam = parameters["RemoteFileActrTypes"] {
      for mapping in remoteFileActrTypesParam.split(separator: ";") {
        let parts = mapping.split(separator: "=", maxSplits: 1)
        if parts.count == 2 {
          let file = String(parts[0])
          let actrType = String(parts[1])
          remoteFileToActrType[file] = actrType
        }
      }
    }

    // First pass: Collect info about all files and identify all Remote services
    var remoteServices: [RemoteServiceInfo] = []
    var localServiceMetadata: [LocalServiceMetadata] = []
    var remoteServiceMetadata: [RemoteServiceMetadata] = []
    for fileDescriptor in request.protoFile {
      let isRemote: Bool
      if remoteFilesParam.contains(fileDescriptor.name) {
        isRemote = true
      } else if localFilesParam.contains(fileDescriptor.name)
        || localFileParam == fileDescriptor.name
      {
        isRemote = false
      } else {
        isRemote = globalProtoSource == "remote"
      }

      for service in fileDescriptor.service {
        let serviceName = service.name
        let methods = service.method.map {
          buildMethodMetadata(
            packageName: fileDescriptor.package,
            serviceName: serviceName,
            methodName: $0.name,
            inputType: $0.inputType,
            outputType: $0.outputType)
        }

        if isRemote {
          let routeKeys = methods.map { $0.route_key }
          remoteServices.append(
            RemoteServiceInfo(
              serviceName: serviceName, routeKeys: routeKeys, fileName: fileDescriptor.name))

          let actrType =
            remoteFileToActrType[fileDescriptor.name]
            ?? "\(manufacturer):\(serviceName):1.0.0"
          remoteServiceMetadata.append(
            RemoteServiceMetadata(
              name: serviceName,
              package: fileDescriptor.package,
              proto_file: fileDescriptor.name,
              actr_type: actrType,
              client_type: "\(serviceName)Client",
              methods: methods))
        } else {
          localServiceMetadata.append(
            LocalServiceMetadata(
              name: serviceName,
              package: fileDescriptor.package,
              proto_file: fileDescriptor.name,
              handler_interface: "\(serviceName)Handler",
              workload_type: "\(serviceName)Workload",
              dispatcher_type: "\(serviceName)Dispatcher",
              methods: methods))
        }
      }
    }

    var response = Google_Protobuf_Compiler_CodeGeneratorResponse()

    // Second pass: Generate content for each file
    for fileDescriptor in request.protoFile {
      // Only generate code for files explicitly requested by protoc
      if !request.fileToGenerate.contains(fileDescriptor.name) { continue }

      let isRemote: Bool
      if remoteFilesParam.contains(fileDescriptor.name) {
        isRemote = true
      } else if localFilesParam.contains(fileDescriptor.name)
        || localFileParam == fileDescriptor.name
      {
        isRemote = false
      } else {
        isRemote = globalProtoSource == "remote"
      }

      // Skip generating files without services and not explicitly local
      if fileDescriptor.service.isEmpty,
        localFileParam != fileDescriptor.name,
        !localFilesParam.contains(fileDescriptor.name)
      {
        continue
      }

      var content = """
        // DO NOT EDIT.
        // Generated by protoc-gen-actrframework-swift

        import Foundation
        import SwiftProtobuf
        import Actr

        """

      let packagePrefix =
        fileDescriptor.package.isEmpty
        ? "" : fileDescriptor.package.split(separator: "_").map { $0.capitalized }.joined() + "_"

      // For Local mode with no services, generate a generic Workload from package name
      if !isRemote, fileDescriptor.service.isEmpty {
        let pkgName =
          fileDescriptor.package.isEmpty
          ? "Client" : fileDescriptor.package.split(separator: "_").map { $0.capitalized }.joined()
        let workloadName = "\(pkgName)Workload"

        content += """

          /// \(pkgName) workload wrapper - automatically generated for empty local proto
          \(accessModifier)actor \(workloadName) {
              private let remoteTargets: [String: ActrType]

              \(accessModifier)init(remoteTargets: [String: ActrType] = [:]) {
                  self.remoteTargets = remoteTargets
              }
          }

          extension \(workloadName) {
              \(accessModifier)func __dispatch(ctx: Context, envelope: RpcEnvelope) async throws -> Data {
                  switch envelope.routeKey {
          """

        // Remote service forwarding cases
        // Group remote services by their actr_type
        var servicesByActrType: [String: [RemoteServiceInfo]] = [:]
        for remoteService in remoteServices {
          let actrType =
            remoteFileToActrType[remoteService.fileName]
            ?? "\(manufacturer):\(remoteService.serviceName):1.0.0"
          if servicesByActrType[actrType] == nil {
            servicesByActrType[actrType] = []
          }
          servicesByActrType[actrType]?.append(remoteService)
        }

        // Generate forwarding cases for each actr_type
        for (_, services) in servicesByActrType.sorted(by: { $0.key < $1.key }) {
          var routeKeysForThisType: [String] = []
          for service in services {
            routeKeysForThisType.append(contentsOf: service.routeKeys)
          }

          if !routeKeysForThisType.isEmpty {
            let routeKeysList = routeKeysForThisType.map { "\"\($0)\"" }.joined(
              separator: ",\n                        ")
            content += """

                      case \(routeKeysList):
                          let targetType = try remoteTargetType(for: envelope.routeKey)
                          let targetId = try await ctx.discover(targetType: targetType)
                          return try await ctx.call(
                              target: targetId,
                              routeKey: envelope.routeKey,
                              payload: envelope.payload
                          )
              """
          }
        }

        content += """

                  default:
                      throw ActrError.UnknownRoute(msg: "Unknown route: \\(envelope.routeKey)")
                  }
              }

              private func remoteTargetType(for routeKey: String) throws -> ActrType {
                  guard let targetType = remoteTargets[routeKey] else {
                      throw ActrError.UnknownRoute(msg: "No remote target configured for route: \\(routeKey)")
                  }
                  return targetType
              }
          }

          """
      } else {
        for service in fileDescriptor.service {
          let serviceName = service.name
          let handlerProtocol = "\(serviceName)Handler"

          // 1. Generate Handler Protocol (Local only)
          if !isRemote {
            content += """

              /// \(serviceName) service handler protocol - users need to implement this protocol
              \(accessModifier)protocol \(handlerProtocol): Sendable {
              """

            for method in service.method {
              let methodName = method.name.prefix(1).lowercased() + method.name.dropFirst()
              let inputType = packagePrefix + method.inputType.split(separator: ".").last!
              let outputType = packagePrefix + method.outputType.split(separator: ".").last!

              content += """

                /// RPC method: \(method.name)
                func \(methodName)(
                    req: \(inputType),
                    ctx: Context
                ) async throws -> \(outputType)

                """
            }

            content += "\n}\n"
          }

          // 2. Generate RpcRequest extensions
          for method in service.method {
            let inputType = packagePrefix + method.inputType.split(separator: ".").last!
            let outputType = packagePrefix + method.outputType.split(separator: ".").last!

            let routeKey: String
            if fileDescriptor.package.isEmpty {
              routeKey = "\(serviceName).\(method.name)"
            } else {
              routeKey = "\(fileDescriptor.package).\(serviceName).\(method.name)"
            }

            content += """

              extension \(inputType): RpcRequest {
                  \(accessModifier)typealias Response = \(outputType)

                  \(accessModifier)static var routeKey: String { "\(routeKey)" }
              }
              """
          }

          // 3. Generate Workload actor (Local only)
          if !isRemote {
            let workloadName = "\(serviceName)Workload"
            content += """

              /// \(serviceName) workload wrapper - wraps the user's handler implementation
              \(accessModifier)actor \(workloadName)<T: \(handlerProtocol)> {
                  \(accessModifier)let handler: T
                  private let remoteTargets: [String: ActrType]

                  \(accessModifier)init(handler: T, remoteTargets: [String: ActrType] = [:]) {
                      self.handler = handler
                      self.remoteTargets = remoteTargets
                  }
              }

              extension \(workloadName) {
                  \(accessModifier)func __dispatch(ctx: Context, envelope: RpcEnvelope) async throws -> Data {
                      switch envelope.routeKey {
              """

            // Local Methods
            for method in service.method {
              let methodName = method.name.prefix(1).lowercased() + method.name.dropFirst()
              let inputType = packagePrefix + method.inputType.split(separator: ".").last!

              let routeKey: String
              if fileDescriptor.package.isEmpty {
                routeKey = "\(serviceName).\(method.name)"
              } else {
                routeKey = "\(fileDescriptor.package).\(serviceName).\(method.name)"
              }

              content += """

                case "\(routeKey)":
                    let req = try \(inputType)(serializedBytes: envelope.payload)
                    let resp = try await handler.\(methodName)(req: req, ctx: ctx)
                    return try resp.serializedData()
                """
            }

            // Remote service forwarding cases
            // Group remote services by their actr_type
            var servicesByActrType: [String: [RemoteServiceInfo]] = [:]
            for remoteService in remoteServices {
              let actrType =
                remoteFileToActrType[remoteService.fileName]
                ?? "\(manufacturer):\(remoteService.serviceName):1.0.0"
              if servicesByActrType[actrType] == nil {
                servicesByActrType[actrType] = []
              }
              servicesByActrType[actrType]?.append(remoteService)
            }

            // Generate forwarding cases for each actr_type
            for (_, services) in servicesByActrType.sorted(by: { $0.key < $1.key }) {
              var routeKeysForThisType: [String] = []
              for service in services {
                routeKeysForThisType.append(contentsOf: service.routeKeys)
              }

              if !routeKeysForThisType.isEmpty {
                let routeKeysList = routeKeysForThisType.map { "\"\($0)\"" }.joined(
                  separator: ",\n                 ")
                content += """

                  case \(routeKeysList):
                      let targetType = try remoteTargetType(for: envelope.routeKey)
                      let targetId = try await ctx.discover(targetType: targetType)
                      return try await ctx.call(
                          target: targetId,
                          routeKey: envelope.routeKey,
                          payload: envelope.payload
                      )
                  """
              }
            }

            content += """

                      default:
                          throw ActrError.UnknownRoute(msg: "Unknown route: \\(envelope.routeKey)")
                      }
                  }

              private func remoteTargetType(for routeKey: String) throws -> ActrType {
                  guard let targetType = remoteTargets[routeKey] else {
                      throw ActrError.UnknownRoute(msg: "No remote target configured for route: \\(routeKey)")
                  }
                  return targetType
              }
              }

              """
          }
        }
      }

      var generatedFile = Google_Protobuf_Compiler_CodeGeneratorResponse.File()
      let suffix = isRemote ? ".client.swift" : ".actor.swift"
      generatedFile.name = fileDescriptor.name.replacingOccurrences(of: ".proto", with: suffix)
      generatedFile.content = content
      response.file.append(generatedFile)
    }

    let metadata = ActrGenMetadata(
      plugin_version: version,
      language: "swift",
      local_services: localServiceMetadata,
      remote_services: remoteServiceMetadata)
    let encoder = JSONEncoder()
    encoder.outputFormatting = [.prettyPrinted, .sortedKeys]
    var metadataFile = Google_Protobuf_Compiler_CodeGeneratorResponse.File()
    metadataFile.name = "actr-gen-meta.json"
    metadataFile.content = String(
      data: try encoder.encode(metadata),
      encoding: .utf8
    ) ?? "{}"
    response.file.append(metadataFile)

    // Write the response back to stdout
    try FileHandle.standardOutput.write(response.serializedData())
  }

  static func buildMethodMetadata(
    packageName: String,
    serviceName: String,
    methodName: String,
    inputType: String,
    outputType: String
  ) -> MethodMetadata {
    let routeKey: String
    if packageName.isEmpty {
      routeKey = "\(serviceName).\(methodName)"
    } else {
      routeKey = "\(packageName).\(serviceName).\(methodName)"
    }

    return MethodMetadata(
      name: methodName,
      snake_name: snakeCase(methodName),
      input_type: shortTypeName(inputType),
      output_type: shortTypeName(outputType),
      route_key: routeKey)
  }

  static func shortTypeName(_ rawType: String) -> String {
    let trimmed = rawType.trimmingCharacters(in: CharacterSet(charactersIn: "."))
    return trimmed.split(separator: ".").last.map(String.init) ?? trimmed
  }

  static func snakeCase(_ value: String) -> String {
    guard !value.isEmpty else { return value }
    var output = ""
    for character in value {
      if character.isUppercase && !output.isEmpty {
        output.append("_")
      }
      output.append(character.lowercased())
    }
    return output
  }
}
