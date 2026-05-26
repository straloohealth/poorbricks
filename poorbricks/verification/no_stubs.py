"""AST lint: detect stub columns in pipeline transforms.

A "stub" is a column projected as a constant (``f.lit(None)`` or
``f.lit(<value>)``) that is also declared in the table's validated
schema — i.e. a column the producing pipeline pretends to populate but
actually leaves as a placeholder. Such columns silently break downstream
gold transforms / dashboards.

Run as a CLI:

    poorbricks-verify-no-stubs <tables_root>

Returns non-zero exit when stubs are found. Wired into
``poorbricks upload --verify`` so any upload re-introducing a stub
fails before propagating.

Detection rules:

    STUB_NULL_COLUMN
        ``f.lit(None).cast(<type>).alias(<col>)`` where ``<col>`` is in
        the same package's ``ValidatedStruct`` model.

    STUB_CONSTANT
        ``f.lit(<constant>).alias(<col>)`` where ``<col>`` is in the
        ``ValidatedStruct`` AND the line (or the line above) carries a
        ``# TODO|stub|placeholder`` marker.

The check is intentionally conservative — a transform that legitimately
defaults a boolean to ``False`` for non-null safety is fine UNLESS it
sits next to a TODO marker.
"""

from __future__ import annotations

import ast
import sys
from dataclasses import dataclass
from pathlib import Path


@dataclass
class StubFinding:
    file: Path
    line: int
    column: str
    rule: str
    detail: str

    def format(self) -> str:
        return (
            f"{self.file}:{self.line} [{self.rule}] column={self.column!r} "
            f"— {self.detail}"
        )


_STUB_COMMENT_TERMS = ("TODO", "stub", "STUB", "placeholder", "PLACEHOLDER")


def _schema_columns_for(transform_path: Path) -> set[str]:
    """Return the set of column names declared in the sibling ``config.py``.

    A ``ValidatedStruct`` config declares its columns as pydantic ``Field``
    attributes. We parse the AST and collect every annotated assignment
    whose target is at class scope.
    """
    config = transform_path.parent / "config.py"
    if not config.exists():
        return set()
    try:
        tree = ast.parse(config.read_text())
    except SyntaxError:
        return set()
    cols: set[str] = set()
    for node in ast.walk(tree):
        if not isinstance(node, ast.ClassDef):
            continue
        for stmt in node.body:
            # Treat annotated assignments inside a class as schema columns.
            if isinstance(stmt, ast.AnnAssign) and isinstance(stmt.target, ast.Name):
                cols.add(stmt.target.id)
    return cols


def _line_or_above_has_marker(source: list[str], lineno: int) -> bool:
    if lineno <= 0 or lineno > len(source):
        return False
    line = source[lineno - 1]
    if any(term in line for term in _STUB_COMMENT_TERMS):
        return True
    if lineno >= 2 and any(term in source[lineno - 2] for term in _STUB_COMMENT_TERMS):
        return True
    return False


def _is_f_lit_call(node: ast.AST) -> ast.Call | None:
    """Match ``f.lit(...)`` (any module attr ``lit``). Returns the Call node."""
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Attribute):
        if node.func.attr == "lit":
            return node
    return None


def _alias_target(node: ast.AST) -> tuple[ast.AST, str] | None:
    """Match ``<inner>.alias(<name>)`` — return ``(inner, name)`` or None."""
    if (
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "alias"
        and node.args
        and isinstance(node.args[0], ast.Constant)
        and isinstance(node.args[0].value, str)
    ):
        return node.func.value, node.args[0].value
    return None


def _walk_chain(node: ast.AST):
    """Yield the chain of nested Call.func.value nodes (the LHS of method chains)."""
    cur = node
    while isinstance(cur, ast.Call) and isinstance(cur.func, ast.Attribute):
        yield cur
        cur = cur.func.value


def find_stubs_in(transform_path: Path) -> list[StubFinding]:
    """Scan a single ``transform.py`` for stub columns."""
    if not transform_path.exists():
        return []
    schema_cols = _schema_columns_for(transform_path)
    if not schema_cols:
        return []
    source_text = transform_path.read_text()
    source_lines = source_text.splitlines()
    try:
        tree = ast.parse(source_text)
    except SyntaxError:
        return []

    findings: list[StubFinding] = []
    for node in ast.walk(tree):
        target = _alias_target(node)
        if target is None:
            continue
        inner, col_name = target
        if col_name not in schema_cols:
            continue
        # Walk down the chain looking for an f.lit(...) anywhere.
        chain_node = inner
        lit_call: ast.Call | None = None
        # The chain may be `f.lit(...).cast(...).alias(col)` so unwrap repeatedly.
        while isinstance(chain_node, ast.Call) and isinstance(
            chain_node.func, ast.Attribute
        ):
            if chain_node.func.attr == "lit":
                lit_call = chain_node
                break
            chain_node = chain_node.func.value
        if lit_call is None:
            continue
        # What constant is being lit'd?
        if not lit_call.args:
            continue
        lit_arg = lit_call.args[0]
        if isinstance(lit_arg, ast.Constant) and lit_arg.value is None:
            findings.append(
                StubFinding(
                    file=transform_path,
                    line=node.lineno,
                    column=col_name,
                    rule="STUB_NULL_COLUMN",
                    detail="schema column projected as f.lit(None) — no real source",
                )
            )
        elif _line_or_above_has_marker(source_lines, node.lineno):
            findings.append(
                StubFinding(
                    file=transform_path,
                    line=node.lineno,
                    column=col_name,
                    rule="STUB_CONSTANT",
                    detail=(
                        "schema column projected as f.lit(<constant>) next to a "
                        "TODO/stub/placeholder marker"
                    ),
                )
            )

    return findings


def find_stubs(tables_root: Path) -> list[StubFinding]:
    """Walk ``<tables_root>/**/transform.py`` and gather every stub finding."""
    out: list[StubFinding] = []
    for path in tables_root.rglob("transform.py"):
        if "__pycache__" in path.parts:
            continue
        out.extend(find_stubs_in(path))
    return out


def main(argv: list[str] | None = None) -> int:
    args = list(argv if argv is not None else sys.argv[1:])
    if not args:
        print("usage: poorbricks-verify-no-stubs <tables_root>", file=sys.stderr)
        return 2
    root = Path(args[0])
    if not root.is_dir():
        print(f"not a directory: {root}", file=sys.stderr)
        return 2
    findings = find_stubs(root)
    if not findings:
        print(f"no stub columns under {root}")
        return 0
    for f in findings:
        print(f.format())
    print(f"\n{len(findings)} stub column(s) found", file=sys.stderr)
    return 1


if __name__ == "__main__":
    sys.exit(main())
