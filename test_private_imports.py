"""
Test to verify that non-test files don't import private functions (functions starting with underscore).
This ensures proper encapsulation and prevents accidental dependencies on internal implementation details.
"""

import ast
from pathlib import Path


def find_python_files() -> list[Path]:
    """Find all Python files in the project, excluding test files."""
    python_files = []

    # Start from the project root
    project_root = Path(".")

    for py_file in project_root.rglob("*.py"):
        # Skip test files (files starting with test_ or ending with _test.py)
        if py_file.name.startswith("test_") or py_file.name.endswith("_test.py"):
            continue

        # Skip __pycache__ and other special directories
        if any(
            part.startswith("__pycache__") or part.startswith(".")
            for part in py_file.parts
        ):
            continue

        python_files.append(py_file)

    return python_files


def extract_imports(file_path: Path) -> list[tuple[str, str, int]]:
    """
    Extract all imports from a Python file and return private function imports.
    Returns list of tuples: (module_name, imported_name, line_number)
    """
    private_imports = []

    try:
        with open(file_path, encoding="utf-8") as f:
            content = f.read()

        tree = ast.parse(content)

        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                if node.names:
                    for alias in node.names:
                        imported_name = alias.name
                        # Check if imported name starts with underscore (private)
                        if imported_name.startswith(
                            "_"
                        ) and not imported_name.startswith("__"):
                            module_name = node.module or ""
                            private_imports.append(
                                (module_name, imported_name, node.lineno)
                            )

            elif isinstance(node, ast.Import):
                for alias in node.names:
                    imported_name = alias.name
                    # Check for private module imports (less common but possible)
                    if "._" in imported_name:
                        private_imports.append(("", imported_name, node.lineno))

    except (SyntaxError, UnicodeDecodeError) as e:
        print(f"Warning: Could not parse {file_path}: {e}")

    return private_imports


def test_no_private_imports():
    """Test that non-test files don't import private functions."""
    python_files = find_python_files()
    violations = []

    for file_path in python_files:
        private_imports = extract_imports(file_path)

        for module_name, imported_name, line_number in private_imports:
            violations.append(
                {
                    "file": str(file_path),
                    "module": module_name,
                    "imported_name": imported_name,
                    "line": line_number,
                }
            )

    if violations:
        error_message = "Found private function imports in non-test files:\n"
        for violation in violations:
            error_message += (
                f"  {violation['file']}:{violation['line']} - "
                f"imports '{violation['imported_name']}' from '{violation['module']}'\n"
            )
        error_message += "\nPrivate functions (starting with _) should not be imported by non-test files."
        raise AssertionError(error_message)


if __name__ == "__main__":
    test_no_private_imports()
    print("✅ All files pass private import check!")
