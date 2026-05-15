"""Drop JVM stack frames + SLF4J noise from subprocess output.

Used wherever we wrap a ``.venv-dbconnect`` subprocess (verify, expectations,
diff_from_prod). Mirrors ``postgres_export.py``'s
``str(exc).splitlines()[0][:120]`` truncation so log volume stays manageable
even when a single pipeline fails.
"""

from __future__ import annotations


def filter_jvm_noise(output: str) -> list[str]:
    """Return ``output`` lines with JVM stack frames + SLF4J banners removed.

    Drops:
    - ``\\tat <classname>...`` Java stack frames
    - ``\\t... N more`` truncated-frame markers
    - ``SLF4J:`` banner lines
    - ``WARNING`` lines (typically log4j config warnings, not real errors)

    Preserves the order of remaining lines.
    """
    return [
        line
        for line in output.splitlines()
        if line.strip()
        and not line.startswith("\tat ")
        and not line.startswith("\t...")
        and "SLF4J" not in line
        and "WARNING" not in line
    ]


def first_error_line(output: str, max_len: int = 120) -> str:
    """Return a single-line summary of the most useful error line in ``output``.

    The last line surviving :func:`filter_jvm_noise` is usually the actual error
    message (Spark/Java prints the exception text after the stack trace). Falls
    back to an empty string when nothing useful survived.
    """
    lines = filter_jvm_noise(output)
    if not lines:
        return ""
    return lines[-1][:max_len]
