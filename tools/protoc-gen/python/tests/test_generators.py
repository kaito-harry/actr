import unittest
from types import SimpleNamespace

from framework_codegen_python.__main__ import build_type_ref, normalize_proto_path
from framework_codegen_python.generators import generate_local_workload_module


class GeneratorTests(unittest.TestCase):
    def test_metadata_helpers_use_canonical_names_and_paths(self) -> None:
        self.assertEqual(normalize_proto_path(r".\remote\ask"), "remote/ask.proto")
        self.assertEqual(normalize_proto_path("./remote/ask.proto"), "remote/ask.proto")

    def test_unresolved_type_error_includes_rpc_context(self) -> None:
        with self.assertRaisesRegex(
            ValueError,
            r"Cannot resolve input type `Missing` for Client\.Call in local/client\.proto",
        ):
            build_type_ref(
                ".Missing",
                {},
                kind="input",
                service_name="Client",
                method_name="Call",
                proto_file="local/client.proto",
            )

    def test_nested_rpc_types_keep_their_owner_relative_path(self) -> None:
        method = SimpleNamespace(
            name="Call",
            input_type=".ask.Outer.InnerRequest",
            output_type=".ask.Outer.InnerResponse",
            client_streaming=False,
            server_streaming=False,
        )
        service = SimpleNamespace(name="ClientService", method=[method])
        type_to_owner = {
            "ask.Outer.InnerRequest": (
                "ask",
                "remote/ask/ask.proto",
                ("Outer", "InnerRequest"),
            ),
            "ask.Outer.InnerResponse": (
                "ask",
                "remote/ask/ask.proto",
                ("Outer", "InnerResponse"),
            ),
        }

        generated = generate_local_workload_module(
            "client",
            "local/client.proto",
            [service],
            type_to_owner,
        )

        self.assertIn(
            "remote_ask_ask_pb2.Outer.InnerRequest.FromString",
            generated["content"],
        )
        self.assertIn(
            "isinstance(resp, remote_ask_ask_pb2.Outer.InnerResponse)",
            generated["content"],
        )


if __name__ == "__main__":
    unittest.main()
