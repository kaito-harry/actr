import Actr
import Testing

@Test func actrTypeStringRepresentationUsesCanonicalColonFormat() throws {
    let type = ActrType(manufacturer: "acme", name: "EchoService", version: "0.1.0")

    #expect(type.toStringRepr() == "acme:EchoService:0.1.0")
    #expect(try ActrType.fromStringRepr("acme:EchoService:0.1.0") == type)
}
