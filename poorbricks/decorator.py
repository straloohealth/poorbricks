"""The ``@pipeline`` decorator.

Registers a pipeline in the framework registry and wraps it with schema
validation via ``@verify_with_model``. The decorated function is callable
with an explicit Inputs instance (local mode) or defaults to building
Inputs from upstream data (production mode).
"""

from __future__ import annotations

import inspect
from collections.abc import Callable
from typing import TYPE_CHECKING, cast, get_type_hints

from pyspark.sql import DataFrame

from validation import verify_with_model

from .inputs import Inputs
from .registry import PipelineMeta, register_pipeline

VALID_STORAGE = {"delta", "postgres"}

if TYPE_CHECKING:
    from validation import ValidatedStruct


def _extract_inputs_class(fn: Callable[..., DataFrame]) -> type[Inputs]:
    """Read the function's first parameter type hint to find the Inputs subclass."""
    sig = inspect.signature(fn)
    params = list(sig.parameters)
    if not params:
        raise TypeError(
            f"@pipeline function {fn.__name__!r} must accept exactly one parameter "
            f"typed as a subclass of .Inputs (got none)."
        )
    if len(params) > 1:
        raise TypeError(
            f"@pipeline function {fn.__name__!r} must accept exactly one parameter "
            f"(got {params})."
        )
    hints = get_type_hints(fn)
    inputs_cls = hints.get(params[0])
    if inputs_cls is None or not (
        isinstance(inputs_cls, type) and issubclass(inputs_cls, Inputs)
    ):
        raise TypeError(
            f"@pipeline function {fn.__name__!r}: parameter {params[0]!r} must be "
            f"typed as a subclass of .Inputs (got {inputs_cls!r})."
        )
    return cast("type[Inputs]", inputs_cls)


def pipeline(
    *,
    name: str,
    model: type[ValidatedStruct],
    level: str,
    comment: str,
    storage: str = "delta",
) -> Callable[[Callable[..., DataFrame]], Callable[..., DataFrame]]:
    """Decorator that registers a poorbricks pipeline.

    Args:
        name: Output table name. For ``storage="delta"`` stores in Delta tables.
            For ``storage="postgres"`` stores in PostgreSQL.
        model: ValidatedStruct describing the output schema.
        level: One of ``"bronze"``, ``"silver"``, ``"gold"``. For Postgres
            pipelines this also names the destination Postgres schema.
        comment: Short human-readable description of the table.
        storage: ``"delta"`` (default) or ``"postgres"``. Determines where
            the table is materialized and how the runner executes the pipeline.

    The decorated function must take exactly one parameter, typed as a
    subclass of ``.Inputs``. The body should call ``transform.compute(inputs)``
    (the actual logic lives in transform.py).
    """
    if level not in {"bronze", "silver", "gold"}:
        raise ValueError(f"level must be 'bronze' | 'silver' | 'gold', got {level!r}")
    if storage not in VALID_STORAGE:
        raise ValueError(
            f"storage must be one of {sorted(VALID_STORAGE)}, got {storage!r}"
        )

    def decorator(fn: Callable[..., DataFrame]) -> Callable[..., DataFrame]:
        inputs_cls = _extract_inputs_class(fn)

        # Wrap with schema validation
        verified: Callable[..., DataFrame] = verify_with_model(model)(fn)

        # Register in the framework registry
        register_pipeline(
            PipelineMeta(
                table_name=name,
                original_fn=fn,
                dlt_fn=verified,
                inputs_cls=inputs_cls,
                model=model,
                level=level,
                comment=comment,
                module=fn.__module__,
                target_storage=storage,
            )
        )
        return verified

    return decorator


__all__ = ["pipeline"]
