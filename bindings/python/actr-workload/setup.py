from __future__ import annotations

from pathlib import Path
from shutil import copy2

from setuptools import setup
from setuptools.command.build_py import build_py
from setuptools.command.sdist import sdist


BUILD_WIT = Path("actr_workload/wit/actr-workload.wit")
SDIST_WIT = Path("src/actr_workload/wit/actr-workload.wit")
REPO_WIT = Path(__file__).resolve().parents[3] / "core/framework/wit/actr-workload.wit"


def source_wit() -> Path:
    local_wit = Path(__file__).resolve().parent / SDIST_WIT
    if REPO_WIT.is_file():
        return REPO_WIT
    if local_wit.is_file():
        return local_wit
    raise FileNotFoundError(f"actr workload WIT not found at {REPO_WIT} or {local_wit}")


def copy_wit(target: Path) -> None:
    target.parent.mkdir(parents=True, exist_ok=True)
    copy2(source_wit(), target)


class BuildPy(build_py):
    def run(self) -> None:
        super().run()
        copy_wit(Path(self.build_lib) / BUILD_WIT)


class Sdist(sdist):
    def make_release_tree(self, base_dir: str, files: list[str]) -> None:
        super().make_release_tree(base_dir, files)
        copy_wit(Path(base_dir) / SDIST_WIT)


setup(cmdclass={"build_py": BuildPy, "sdist": Sdist})
