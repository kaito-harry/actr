#!/usr/bin/env python3
from __future__ import annotations

import unittest

import ci_targets


class CiTargetsTest(unittest.TestCase):
    def test_c_and_go_echo_workloads_trigger_component_builds(self) -> None:
        paths = (
            "examples/c/echo-workload/src/main.c",
            "examples/go/echo-workload/build.sh",
        )

        for path in paths:
            with self.subTest(path=path):
                targets, reasons = ci_targets.detect_targets([path], full_run=False)

                self.assertTrue(targets["c_go_workloads"])
                self.assertIn(f"c_go_workload:{path}", reasons)

    def test_v2_wit_change_rebuilds_c_and_go_components(self) -> None:
        path = "core/framework/wit-v2/actr-workload.wit"
        targets, reasons = ci_targets.detect_targets([path], full_run=False)

        self.assertTrue(targets["c_go_workloads"])
        self.assertTrue(targets["rust_core"])
        self.assertIn(f"core_dependency:{path}", reasons)

    def test_typescript_echo_workload_triggers_workload_checks(self) -> None:
        targets, reasons = ci_targets.detect_targets(
            ["examples/typescript/echo-workload/src/actr_service.ts"],
            full_run=False,
        )

        self.assertTrue(targets["ts_workload"])
        self.assertFalse(targets["web_binding"])
        self.assertIn(
            "typescript_workload:examples/typescript/echo-workload/src/actr_service.ts",
            reasons,
        )

    def test_generated_typescript_browser_e2e_source_triggers_light_checks(self) -> None:
        targets, reasons = ci_targets.detect_targets(
            ["cli/tests/e2e_typescript_generated_echo_web.rs"],
            full_run=False,
        )

        self.assertTrue(targets["rust_core"])
        self.assertTrue(targets["ts_workload"])
        self.assertTrue(targets["web_binding"])
        self.assertIn(
            "generated_typescript_web_e2e_source:cli/tests/e2e_typescript_generated_echo_web.rs",
            reasons,
        )

    def test_kotlin_codegen_paths_trigger_rust_and_kotlin_checks(self) -> None:
        paths = (
            "cli/src/commands/codegen/kotlin.rs",
            "cli/tests/kotlin_echo.rs",
            "cli/fixtures/kotlin/echo/MainActivity.kt",
        )

        for path in paths:
            with self.subTest(path=path):
                targets, reasons = ci_targets.detect_targets([path], full_run=False)

                self.assertTrue(targets["rust_core"])
                self.assertTrue(targets["kotlin_binding"])
                self.assertIn(f"kotlin_codegen:{path}", reasons)

    def test_v2_workload_wit_triggers_sdk_checks(self) -> None:
        path = "core/framework/wit-v2/actr-workload.wit"
        targets, reasons = ci_targets.detect_targets([path], full_run=False)

        self.assertTrue(targets["rust_core"])
        self.assertTrue(targets["python_workload"])
        self.assertTrue(targets["ts_workload"])
        self.assertIn(f"core_dependency:{path}", reasons)

    def test_retained_web_wit_does_not_trigger_workload_sdk_checks(self) -> None:
        path = "core/framework/wit/actr-workload.wit"
        targets, _ = ci_targets.detect_targets([path], full_run=False)

        self.assertTrue(targets["rust_core"])
        self.assertFalse(targets["python_workload"])
        self.assertFalse(targets["ts_workload"])


if __name__ == "__main__":
    unittest.main()
