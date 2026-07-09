import Foundation
import SwiftProtobuf
import XCTest

@testable import framework_codegen_swift

final class MetadataTests: XCTestCase {
  func testMethodMetadataEncodesTypeRefsInsteadOfLegacyTypeFields() throws {
    var innerRequest = Google_Protobuf_DescriptorProto()
    innerRequest.name = "InnerRequest"
    var innerResponse = Google_Protobuf_DescriptorProto()
    innerResponse.name = "InnerResponse"
    var outer = Google_Protobuf_DescriptorProto()
    outer.name = "Outer"
    outer.nestedType = [innerRequest, innerResponse]

    var typeRefs: [String: ActrFrameworkGenerator.TypeRef] = [:]
    ActrFrameworkGenerator.collectTypeRefs(
      package: "ask",
      protoFile: "remote/ask/ask.proto",
      message: outer,
      parentProtoName: nil,
      into: &typeRefs)

    let method = try ActrFrameworkGenerator.buildMethodMetadata(
      ActrFrameworkGenerator.MethodMetadataInput(
        packageName: "client",
        serviceName: "Client",
        methodName: "Foo",
        inputType: ".ask.Outer.InnerRequest",
        outputType: ".ask.Outer.InnerResponse"),
      typeRefs: typeRefs)

    let json = try encodedJSON(method)
    XCTAssertNil(json["input_type"])
    XCTAssertNil(json["output_type"])

    let inputRef = try XCTUnwrap(json["input_ref"] as? [String: Any])
    XCTAssertEqual(inputRef["proto_type"] as? String, "ask.Outer.InnerRequest")
    XCTAssertEqual(inputRef["type_name"] as? String, "Outer.InnerRequest")
    XCTAssertEqual(inputRef["proto_package"] as? String, "ask")
    XCTAssertEqual(inputRef["proto_file"] as? String, "remote/ask/ask.proto")

    let outputRef = try XCTUnwrap(json["output_ref"] as? [String: Any])
    XCTAssertEqual(outputRef["proto_type"] as? String, "ask.Outer.InnerResponse")
    XCTAssertEqual(outputRef["type_name"] as? String, "Outer.InnerResponse")
    XCTAssertEqual(outputRef["proto_package"] as? String, "ask")
    XCTAssertEqual(outputRef["proto_file"] as? String, "remote/ask/ask.proto")
  }

  func testMetadataHelpersUseCanonicalNamesAndPaths() {
    XCTAssertEqual(ActrFrameworkGenerator.snakeCase("HTTPServer"), "http_server")
    XCTAssertEqual(ActrFrameworkGenerator.lowerCamelCase("HTTPServer"), "httpServer")
    XCTAssertEqual(
      ActrFrameworkGenerator.normalizeProtoPath(".\\remote\\ask"),
      "remote/ask.proto")
  }

  func testPayloadTypeUsesMethodOptionAndStreamingDefault() throws {
    var signalMethod = Google_Protobuf_MethodDescriptorProto()
    signalMethod.name = "Signal"
    signalMethod.options = try methodOptionsWithPayloadType(1)

    let explicitPayloadType = try ActrFrameworkGenerator.payloadType(
      for: signalMethod,
      packageName: "echo",
      serviceName: "EchoService")
    XCTAssertEqual(explicitPayloadType, .rpcSignal)
    XCTAssertEqual(explicitPayloadType.swiftCase, ".rpcSignal")

    var streamingMethod = Google_Protobuf_MethodDescriptorProto()
    streamingMethod.name = "Upload"
    streamingMethod.clientStreaming = true

    XCTAssertEqual(
      try ActrFrameworkGenerator.payloadType(
        for: streamingMethod,
        packageName: "echo",
        serviceName: "EchoService"),
      .streamReliable)
  }

  func testUnsupportedPayloadTypeReportsMethodContext() throws {
    var method = Google_Protobuf_MethodDescriptorProto()
    method.name = "Broken"
    method.options = try methodOptionsWithPayloadType(99)

    XCTAssertThrowsError(
      try ActrFrameworkGenerator.payloadType(
        for: method,
        packageName: "echo",
        serviceName: "EchoService")
    ) { error in
      XCTAssertEqual(
        error.localizedDescription,
        "Unsupported (actr.payload_type) value 99 for EchoService.Broken"
      )
    }
  }

  func testUnresolvedTypeErrorPreservesTrailingDotAndIncludesContext() {
    XCTAssertThrowsError(
      try ActrFrameworkGenerator.resolveTypeRef(
        ".Missing.",
        kind: "input",
        serviceName: "Client",
        methodName: "Call",
        typeRefs: [:])
    ) { error in
      XCTAssertEqual(
        error.localizedDescription,
        "Cannot resolve input type `Missing.` for Client.Call: RPC types must be declared in one of the parsed proto files"
      )
    }
  }

  private func encodedJSON<T: Encodable>(_ value: T) throws -> [String: Any] {
    let data = try JSONEncoder().encode(value)
    return try XCTUnwrap(JSONSerialization.jsonObject(with: data) as? [String: Any])
  }

  private func methodOptionsWithPayloadType(_ payloadType: UInt64) throws -> Google_Protobuf_MethodOptions {
    var bytes: [UInt8] = []
    appendVarint(UInt64(50_001 << 3), to: &bytes)
    appendVarint(payloadType, to: &bytes)
    return try Google_Protobuf_MethodOptions(serializedBytes: Data(bytes))
  }

  private func appendVarint(_ value: UInt64, to bytes: inout [UInt8]) {
    var remaining = value
    while remaining >= 0x80 {
      bytes.append(UInt8(remaining & 0x7f) | 0x80)
      remaining >>= 7
    }
    bytes.append(UInt8(remaining))
  }
}
