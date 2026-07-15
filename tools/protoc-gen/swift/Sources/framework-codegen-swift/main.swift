import Foundation
import SwiftProtobuf
import SwiftProtobufPluginLibrary

@main
struct ActrFrameworkGenerator {
  static let version = "0.5.0"

  struct RemoteServiceInfo {
    let serviceName: String
    let routeKeys: [String]
    let fileName: String
  }

  struct MethodMetadataInput {
    let packageName: String
    let serviceName: String
    let methodName: String
    let inputType: String
    let outputType: String
  }

  struct MethodMetadata: Encodable {
    let name: String
    let snake_name: String
    let route_key: String
    let input_ref: TypeRef
    let output_ref: TypeRef
  }

  struct TypeRef: Encodable {
    let proto_type: String
    let type_name: String
    let proto_package: String
    let proto_file: String
  }

  enum MetadataError: LocalizedError {
    case unresolvedType(kind: String, typeName: String, serviceName: String, methodName: String)
    case malformedPayloadTypeOption(serviceName: String, methodName: String, detail: String)
    case unsupportedPayloadType(value: UInt64, serviceName: String, methodName: String)

    var errorDescription: String? {
      switch self {
      case let .unresolvedType(kind, typeName, serviceName, methodName):
        return
          "Cannot resolve \(kind) type `\(typeName)` for \(serviceName).\(methodName): RPC types must be declared in one of the parsed proto files"
      case let .malformedPayloadTypeOption(serviceName, methodName, detail):
        return
          "Cannot parse (actr.payload_type) option for \(serviceName).\(methodName): \(detail)"
      case let .unsupportedPayloadType(value, serviceName, methodName):
        return
          "Unsupported (actr.payload_type) value \(value) for \(serviceName).\(methodName)"
      }
    }
  }

  enum RpcPayloadType: UInt64 {
    case rpcReliable = 0
    case rpcSignal = 1
    case streamReliable = 2
    case streamLatencyFirst = 3
    case mediaRtp = 4

    var swiftCase: String {
      switch self {
      case .rpcReliable:
        return ".rpcReliable"
      case .rpcSignal:
        return ".rpcSignal"
      case .streamReliable:
        return ".streamReliable"
      case .streamLatencyFirst:
        return ".streamLatencyFirst"
      case .mediaRtp:
        return ".mediaRtp"
      }
    }
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

    let localFileParam = parameters["LocalFile"].map(normalizeProtoPath)
    let localFilesParam = Set(
      (parameters["LocalFiles"] ?? "").split(separator: ":").map {
        normalizeProtoPath(String($0))
      })
    let protoSourceParam = parameters["ProtoSource"]?.lowercased()
    let globalProtoSource =
      protoSourceParam ?? ((localFileParam != nil || !localFilesParam.isEmpty) ? "remote" : "local")

    let remoteFilesParam = Set(
      (parameters["RemoteFiles"] ?? "").split(separator: ":").map {
        normalizeProtoPath(String($0))
      })

    // Parse RemoteFileActrTypes parameter: file1=actr_type1;file2=actr_type2
    // The top-level protoc parameter string is comma-separated, so mappings
    // inside one parameter use semicolons.
    var remoteFileToActrType: [String: String] = [:]
    if let remoteFileActrTypesParam = parameters["RemoteFileActrTypes"] {
      for mapping in remoteFileActrTypesParam.split(separator: ";") {
        let parts = mapping.split(separator: "=", maxSplits: 1)
        if parts.count == 2 {
          let file = normalizeProtoPath(String(parts[0]))
          let actrType = String(parts[1])
          remoteFileToActrType[file] = actrType
        }
      }
    }

    var typeRefs: [String: TypeRef] = [:]
    for fileDescriptor in request.protoFile {
      for message in fileDescriptor.messageType {
        Self.collectTypeRefs(
          package: fileDescriptor.package,
          protoFile: normalizeProtoPath(fileDescriptor.name),
          message: message,
          parentProtoName: nil,
          into: &typeRefs)
      }
    }

    // First pass: Collect info about all files and identify all Remote services
    var remoteServices: [RemoteServiceInfo] = []
    var localServiceMetadata: [LocalServiceMetadata] = []
    var remoteServiceMetadata: [RemoteServiceMetadata] = []
    for fileDescriptor in request.protoFile {
      let protoFile = normalizeProtoPath(fileDescriptor.name)
      let isRemote: Bool
      if remoteFilesParam.contains(protoFile) {
        isRemote = true
      } else if localFilesParam.contains(protoFile)
        || localFileParam == protoFile
      {
        isRemote = false
      } else {
        isRemote = globalProtoSource == "remote"
      }

      for service in fileDescriptor.service {
        let serviceName = service.name
        let methods = try service.method.map {
          try buildMethodMetadata(
            MethodMetadataInput(
              packageName: fileDescriptor.package,
              serviceName: serviceName,
              methodName: $0.name,
              inputType: $0.inputType,
              outputType: $0.outputType),
            typeRefs: typeRefs)
        }

        if isRemote {
          let routeKeys = methods.map { $0.route_key }
          remoteServices.append(
            RemoteServiceInfo(
              serviceName: serviceName, routeKeys: routeKeys, fileName: protoFile))

          let actrType =
            remoteFileToActrType[protoFile]
            ?? "\(manufacturer):\(serviceName):1.0.0"
          remoteServiceMetadata.append(
            RemoteServiceMetadata(
              name: serviceName,
              package: fileDescriptor.package,
              proto_file: protoFile,
              actr_type: actrType,
              client_type: "\(serviceName)Client",
              methods: methods))
        } else {
          localServiceMetadata.append(
            LocalServiceMetadata(
              name: serviceName,
              package: fileDescriptor.package,
              proto_file: protoFile,
              handler_interface: "\(serviceName)Handler",
              workload_type: "\(serviceName)Workload",
              dispatcher_type: "\(serviceName)Dispatcher",
              methods: methods))
        }
      }
    }

    var response = Google_Protobuf_Compiler_CodeGeneratorResponse()

    // Build a fully-qualified message name -> generated Swift type map from
    // the full descriptor set. Imported RPC message types must resolve to their
    // declaring package's Swift prefix (e.g. `ask.X` -> `Ask_X`) instead of the
    // current service's package.
    var typeToSwiftName: [String: String] = [:]
    for fileDescriptor in request.protoFile {
      for message in fileDescriptor.messageType {
        Self.collectSwiftTypeNames(
          package: fileDescriptor.package,
          message: message,
          parentProtoName: nil,
          parentSwiftName: nil,
          into: &typeToSwiftName)
      }
    }

    // Second pass: Generate content for each file
    for fileDescriptor in request.protoFile {
      // Only generate code for files explicitly requested by protoc
      if !request.fileToGenerate.contains(fileDescriptor.name) { continue }

      let protoFile = normalizeProtoPath(fileDescriptor.name)
      let isRemote: Bool
      if remoteFilesParam.contains(protoFile) {
        isRemote = true
      } else if localFilesParam.contains(protoFile)
        || localFileParam == protoFile
      {
        isRemote = false
      } else {
        isRemote = globalProtoSource == "remote"
      }

      // Skip generating files without services and not explicitly local
      if fileDescriptor.service.isEmpty,
        localFileParam != protoFile,
        !localFilesParam.contains(protoFile)
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
              \(accessModifier)func __dispatch(ctx: any ActrContext, envelope: RpcEnvelope) async throws(ActrError) -> Data {
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

              private func remoteTargetType(for routeKey: String) throws(ActrError) -> ActrType {
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
              let methodName = Self.lowerCamelCase(method.name)
              let inputType = Self.swiftTypeName(
                method.inputType, currentPackage: fileDescriptor.package,
                typeToSwiftName: typeToSwiftName)
              let outputType = Self.swiftTypeName(
                method.outputType, currentPackage: fileDescriptor.package,
                typeToSwiftName: typeToSwiftName)

              content += """

                /// RPC method: \(method.name)
                func \(methodName)(
                    req: \(inputType),
                    ctx: any ActrContext
                ) async throws(ActrError) -> \(outputType)

                """
            }

            content += "\n}\n"
          }

          // 2. Generate RpcRequest extensions
          for method in service.method {
            let inputType = Self.swiftTypeName(
              method.inputType, currentPackage: fileDescriptor.package,
              typeToSwiftName: typeToSwiftName)
            let outputType = Self.swiftTypeName(
              method.outputType, currentPackage: fileDescriptor.package,
              typeToSwiftName: typeToSwiftName)
            let payloadType = try Self.payloadType(
              for: method,
              packageName: fileDescriptor.package,
              serviceName: serviceName)

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
                  \(accessModifier)static var payloadType: PayloadType { \(payloadType.swiftCase) }
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
                  \(accessModifier)func __dispatch(ctx: any ActrContext, envelope: RpcEnvelope) async throws(ActrError) -> Data {
                      switch envelope.routeKey {
              """

            // Local Methods
            for method in service.method {
              let methodName = Self.lowerCamelCase(method.name)
              let inputType = Self.swiftTypeName(
                method.inputType, currentPackage: fileDescriptor.package,
                typeToSwiftName: typeToSwiftName)
              let outputType = Self.swiftTypeName(
                method.outputType, currentPackage: fileDescriptor.package,
                typeToSwiftName: typeToSwiftName)

              let routeKey: String
              if fileDescriptor.package.isEmpty {
                routeKey = "\(serviceName).\(method.name)"
              } else {
                routeKey = "\(fileDescriptor.package).\(serviceName).\(method.name)"
              }

              content += """

                case "\(routeKey)":
                    let req: \(inputType)
                    do {
                        req = try \(inputType)(serializedBytes: envelope.payload)
                    } catch {
                        throw ActrError.DecodeFailure(msg: "Failed to decode \(inputType) for route \\(envelope.routeKey): \\(error)")
                    }
                    let resp = try await handler.\(methodName)(req: req, ctx: ctx)
                    do {
                        return try resp.serializedData()
                    } catch {
                        throw ActrError.DecodeFailure(msg: "Failed to encode \(outputType) for route \\(envelope.routeKey): \\(error)")
                    }
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

              private func remoteTargetType(for routeKey: String) throws(ActrError) -> ActrType {
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
    metadataFile.content =
      String(
        data: try encoder.encode(metadata),
        encoding: .utf8
      ) ?? "{}"
    response.file.append(metadataFile)

    // Write the response back to stdout
    try FileHandle.standardOutput.write(response.serializedData())
  }

  static func buildMethodMetadata(
    _ input: MethodMetadataInput,
    typeRefs: [String: TypeRef]
  ) throws -> MethodMetadata {
    let routeKey: String
    if input.packageName.isEmpty {
      routeKey = "\(input.serviceName).\(input.methodName)"
    } else {
      routeKey = "\(input.packageName).\(input.serviceName).\(input.methodName)"
    }

    return MethodMetadata(
      name: input.methodName,
      snake_name: snakeCase(input.methodName),
      route_key: routeKey,
      input_ref: try resolveTypeRef(
        input.inputType,
        kind: "input",
        serviceName: input.serviceName,
        methodName: input.methodName,
        typeRefs: typeRefs),
      output_ref: try resolveTypeRef(
        input.outputType,
        kind: "output",
        serviceName: input.serviceName,
        methodName: input.methodName,
        typeRefs: typeRefs))
  }

  static func payloadType(
    for method: Google_Protobuf_MethodDescriptorProto,
    packageName _: String,
    serviceName: String
  ) throws -> RpcPayloadType {
    if let rawValue = try extractPayloadTypeOption(
      from: method.options.unknownFields.data,
      serviceName: serviceName,
      methodName: method.name)
    {
      guard let payloadType = RpcPayloadType(rawValue: rawValue) else {
        throw MetadataError.unsupportedPayloadType(
          value: rawValue,
          serviceName: serviceName,
          methodName: method.name)
      }
      return payloadType
    }

    if method.clientStreaming || method.serverStreaming {
      return .streamReliable
    }

    return .rpcReliable
  }

  static func extractPayloadTypeOption(
    from unknownFields: Data,
    serviceName: String,
    methodName: String
  ) throws -> UInt64? {
    let payloadTypeFieldNumber = 50_001
    var index = unknownFields.startIndex

    while index < unknownFields.endIndex {
      let key = try readVarint(
        from: unknownFields,
        index: &index,
        serviceName: serviceName,
        methodName: methodName)
      let fieldNumber = Int(key >> 3)
      let wireType = Int(key & 0x7)

      if fieldNumber == payloadTypeFieldNumber {
        guard wireType == 0 else {
          throw MetadataError.malformedPayloadTypeOption(
            serviceName: serviceName,
            methodName: methodName,
            detail: "expected varint wire type, got \(wireType)")
        }
        return try readVarint(
          from: unknownFields,
          index: &index,
          serviceName: serviceName,
          methodName: methodName)
      }

      try skipUnknownField(
        wireType: wireType,
        in: unknownFields,
        index: &index,
        serviceName: serviceName,
        methodName: methodName)
    }

    return nil
  }

  static func readVarint(
    from data: Data,
    index: inout Data.Index,
    serviceName: String,
    methodName: String
  ) throws -> UInt64 {
    var result: UInt64 = 0
    var shift: UInt64 = 0

    while index < data.endIndex, shift < 64 {
      let byte = data[index]
      index = data.index(after: index)
      result |= UInt64(byte & 0x7f) << shift
      if byte < 0x80 {
        return result
      }
      shift += 7
    }

    throw MetadataError.malformedPayloadTypeOption(
      serviceName: serviceName,
      methodName: methodName,
      detail: "unterminated varint")
  }

  static func skipUnknownField(
    wireType: Int,
    in data: Data,
    index: inout Data.Index,
    serviceName: String,
    methodName: String
  ) throws {
    func advance(by count: Int) throws {
      guard let next = data.index(index, offsetBy: count, limitedBy: data.endIndex) else {
        throw MetadataError.malformedPayloadTypeOption(
          serviceName: serviceName,
          methodName: methodName,
          detail: "truncated unknown field")
      }
      index = next
    }

    switch wireType {
    case 0:
      _ = try readVarint(
        from: data,
        index: &index,
        serviceName: serviceName,
        methodName: methodName)
    case 1:
      try advance(by: 8)
    case 2:
      let length = try readVarint(
        from: data,
        index: &index,
        serviceName: serviceName,
        methodName: methodName)
      guard length <= UInt64(Int.max) else {
        throw MetadataError.malformedPayloadTypeOption(
          serviceName: serviceName,
          methodName: methodName,
          detail: "length-delimited field is too large")
      }
      try advance(by: Int(length))
    case 5:
      try advance(by: 4)
    default:
      throw MetadataError.malformedPayloadTypeOption(
        serviceName: serviceName,
        methodName: methodName,
        detail: "unsupported wire type \(wireType)")
    }
  }

  static func resolveTypeRef(
    _ rawType: String,
    kind: String,
    serviceName: String,
    methodName: String,
    typeRefs: [String: TypeRef]
  ) throws -> TypeRef {
    let trimmed = String(rawType.drop(while: { $0 == "." }))
    if let typeRef = typeRefs[trimmed] {
      return typeRef
    }
    throw MetadataError.unresolvedType(
      kind: kind,
      typeName: trimmed,
      serviceName: serviceName,
      methodName: methodName)
  }

  /// Resolve a fully-qualified proto type (as carried by
  /// `MethodDescriptorProto.inputType`/`outputType`, e.g.
  /// `.ask.ContinuePromptResultStreamsRequest`) to its Swift type name, using
  /// the declaring package's prefix when the type is imported from another
  /// proto file. Falls back to `currentPackage` for types not found in the
  /// descriptor set (well-known types, external imports).
  static func swiftTypeName(
    _ inputType: String,
    currentPackage: String,
    typeToSwiftName: [String: String]
  ) -> String {
    let trimmed = String(inputType.drop(while: { $0 == "." }))
    if let swiftName = typeToSwiftName[trimmed] {
      return swiftName
    }
    let prefix = swiftPackagePrefix(currentPackage)
    let typeName = trimmed.split(separator: ".").last.map(String.init) ?? trimmed
    return prefix + typeName
  }

  static func collectSwiftTypeNames(
    package: String,
    message: Google_Protobuf_DescriptorProto,
    parentProtoName: String?,
    parentSwiftName: String?,
    into typeToSwiftName: inout [String: String]
  ) {
    let protoName = parentProtoName.map { "\($0).\(message.name)" } ?? message.name
    let fullProtoName = package.isEmpty ? protoName : "\(package).\(protoName)"
    let swiftName =
      parentSwiftName.map { "\($0).\(message.name)" }
      ?? "\(swiftPackagePrefix(package))\(message.name)"
    typeToSwiftName[fullProtoName] = swiftName

    for nested in message.nestedType {
      collectSwiftTypeNames(
        package: package,
        message: nested,
        parentProtoName: protoName,
        parentSwiftName: swiftName,
        into: &typeToSwiftName)
    }
  }

  static func collectTypeRefs(
    package: String,
    protoFile: String,
    message: Google_Protobuf_DescriptorProto,
    parentProtoName: String?,
    into typeRefs: inout [String: TypeRef]
  ) {
    let protoName = parentProtoName.map { "\($0).\(message.name)" } ?? message.name
    let fullProtoName = package.isEmpty ? protoName : "\(package).\(protoName)"
    typeRefs[fullProtoName] = TypeRef(
      proto_type: fullProtoName,
      type_name: protoName,
      proto_package: package,
      proto_file: normalizeProtoPath(protoFile))

    for nested in message.nestedType {
      collectTypeRefs(
        package: package,
        protoFile: protoFile,
        message: nested,
        parentProtoName: protoName,
        into: &typeRefs)
    }
  }

  static func swiftPackagePrefix(_ packageName: String) -> String {
    if packageName.isEmpty { return "" }
    let components = packageName.split(separator: ".").map { component in
      component.split(separator: "_").map { $0.capitalized }.joined()
    }
    return components.joined(separator: "_") + "_"
  }

  static func snakeCase(_ value: String) -> String {
    let acronymBoundaries = value.replacingOccurrences(
      of: #"(.)([A-Z][a-z]+)"#,
      with: "$1_$2",
      options: .regularExpression)
    return acronymBoundaries.replacingOccurrences(
      of: #"([a-z0-9])([A-Z])"#,
      with: "$1_$2",
      options: .regularExpression
    ).lowercased()
  }

  static func lowerCamelCase(_ value: String) -> String {
    let parts = snakeCase(value).split(separator: "_")
    guard let first = parts.first else { return "" }
    return String(first) + parts.dropFirst().map { $0.capitalized }.joined()
  }

  static func normalizeProtoPath(_ value: String) -> String {
    var path = value.replacingOccurrences(of: "\\", with: "/")
    while path.hasPrefix("./") {
      path.removeFirst(2)
    }
    return path.hasSuffix(".proto") ? path : "\(path).proto"
  }
}
