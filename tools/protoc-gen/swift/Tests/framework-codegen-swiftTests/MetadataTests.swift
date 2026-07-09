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
}
