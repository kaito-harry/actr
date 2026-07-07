@testable import Actr
import Foundation
import ActrBindings
import SwiftProtobuf
import Testing

private final class InvalidResponseActrRefWrapper: ActrBindings.ActrRefWrapper, @unchecked Sendable {
    init() {
        super.init(noHandle: ActrBindings.ActrRefWrapper.NoHandle())
    }

    required init(unsafeFromHandle handle: UInt64) {
        super.init(unsafeFromHandle: handle)
    }

    override func call(
        routeKey _: String,
        payloadType _: ActrBindings.PayloadType,
        requestPayload _: Data,
        timeoutMs _: Int64
    ) async throws -> Data {
        Data([0xff])
    }
}

extension Google_Protobuf_Empty: RpcRequest {
    public typealias Response = Google_Protobuf_Empty

    public static var routeKey: String {
        "test.Empty/Echo"
    }
}

@Test func typedCallWrapsResponseDecodeFailuresAsActrError() async throws {
    let ref = ActrRef(inner: InvalidResponseActrRefWrapper())

    do {
        _ = try await ref.call(Google_Protobuf_Empty())
        Issue.record("Expected typed call to wrap the response decode failure")
    } catch let error as Actr.ActrError {
        guard case let .Internal(msg) = error else {
            Issue.record("Expected internal ActrError, got \(error)")
            return
        }
        #expect(!msg.isEmpty)
    } catch {
        Issue.record("Expected ActrError, got \(type(of: error))")
    }
}
