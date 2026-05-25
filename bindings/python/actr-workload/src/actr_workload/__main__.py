from __future__ import annotations

import argparse
from pathlib import Path
from typing import Sequence

from .constants import DEFAULT_WORLD, DEFAULT_WORLD_MODULE
from .build import (
    componentize,
    generate_bindings,
)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="actr-workload",
        description="Build helpers for Python actr workload components.",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    bindings_parser = subparsers.add_parser(
        "bindings",
        help="Generate Python bindings for the actr workload WIT world.",
    )
    bindings_parser.add_argument("out_dir", type=Path)
    bindings_parser.add_argument("--wit", type=Path)
    bindings_parser.add_argument("--world", default=DEFAULT_WORLD)
    bindings_parser.add_argument("--world-module", default=DEFAULT_WORLD_MODULE)

    componentize_parser = subparsers.add_parser(
        "componentize",
        help="Componentize a Python workload module.",
    )
    componentize_parser.add_argument("module_name")
    componentize_parser.add_argument("-o", "--output", required=True, type=Path)
    componentize_parser.add_argument("--project-dir", default=Path("."), type=Path)
    componentize_parser.add_argument("--bindings-dir", default=Path("bindings"), type=Path)
    componentize_parser.add_argument("--wit", type=Path)
    componentize_parser.add_argument("--world", default=DEFAULT_WORLD)
    componentize_parser.add_argument("--world-module", default=DEFAULT_WORLD_MODULE)
    componentize_parser.add_argument(
        "--python-path",
        action="append",
        default=[],
        type=Path,
        help="Additional Python import path passed through to componentize-py.",
    )

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    if args.command == "bindings":
        generate_bindings(
            args.out_dir,
            wit=args.wit,
            world=args.world,
            world_module=args.world_module,
        )
        return 0

    if args.command == "componentize":
        componentize(
            args.module_name,
            args.output,
            project_dir=args.project_dir,
            bindings_dir=args.bindings_dir,
            wit=args.wit,
            world=args.world,
            world_module=args.world_module,
            python_paths=args.python_path,
        )
        return 0

    parser.error(f"unknown command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
