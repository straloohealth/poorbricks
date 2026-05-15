#!/usr/bin/env python3
"""Pipeline structure validation script for pre-commit hooks."""

import sys
from pathlib import Path


def check_pipeline_structure() -> bool:
    """Check that all pipelines follow the required file structure."""
    pipelines_dir = Path("tables")
    if not pipelines_dir.exists():
        return True

    errors = []
    for pipeline_dir in pipelines_dir.rglob("*/"):
        if pipeline_dir.name in ["__pycache__", ".pytest_cache", ".cursor"]:
            continue

        # Check if this looks like a pipeline directory
        has_pipeline_py = (pipeline_dir / "pipeline.py").exists()
        has_config_py = (pipeline_dir / "config.py").exists()

        if has_pipeline_py or has_config_py:
            required_files = [
                "__init__.py",
                "config.py",
                "pipeline.py",
                "transform.py",
                "fixtures.py",
                "test_pipeline.py",
            ]
            for required_file in required_files:
                if not (pipeline_dir / required_file).exists():
                    errors.append(f"Missing {required_file} in {pipeline_dir}")

    if errors:
        print("Pipeline structure errors:")
        for error in errors:
            print(f"  - {error}")
        return False
    return True


if __name__ == "__main__":
    if not check_pipeline_structure():
        sys.exit(1)
