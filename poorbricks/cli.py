"""Single ``poorbricks`` CLI dispatcher.

Subcommands:

* ``poorbricks verify`` — see ``poorbricks.verify``.
* ``poorbricks run`` — see ``poorbricks.runner``.
* ``poorbricks upload`` — see ``poorbricks.upload_client``.
"""

from __future__ import annotations

import argparse
import sys


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="poorbricks",
        description="Poorbricks pipeline framework CLI",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
    subparsers.add_parser(
        "verify", help="verify contracts / expectations", add_help=False
    )
    subparsers.add_parser("run", help="run a pipeline locally", add_help=False)
    subparsers.add_parser(
        "upload",
        help="upload pipelines + workflows to a framework server",
        add_help=False,
    )

    args = argv if argv is not None else sys.argv[1:]
    if not args or args[0] in {"-h", "--help"}:
        parser.print_help()
        return 0

    command, rest = args[0], args[1:]

    if command == "verify":
        from poorbricks.verify import main as verify_main

        verify_main(rest)  # exits via sys.exit on its own
        return 0
    if command == "run":
        from poorbricks.runner import main as run_main

        return run_main(rest)
    if command == "upload":
        from poorbricks.upload_client import main as upload_main

        return upload_main(rest)

    parser.error(f"unknown command: {command!r}")  # noqa: ARG002


if __name__ == "__main__":
    sys.exit(main())


__all__ = ["main"]
