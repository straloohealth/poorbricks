"""Named fault transforms for ``MODE=fault FAULT=<name>``.

A fault function takes a fully-resolved ``Inputs`` instance and returns a
mutated copy whose upstream DataFrames are deliberately broken. The runner
then calls ``transform.compute(faulty_inputs)`` so a developer (or AI agent)
can see how the pipeline degrades — does it raise? produce wrong values?
silently drop rows? — without having to deploy first.

Add new faults here and they become available in every pipeline immediately.
"""

from __future__ import annotations

from collections.abc import Callable

from pyspark.sql import functions as f

from .inputs import Inputs

FaultFn = Callable[[Inputs], Inputs]

_faults: dict[str, FaultFn] = {}


def fault(name: str) -> Callable[[FaultFn], FaultFn]:
    """Register a named fault transform."""

    def decorator(fn: FaultFn) -> FaultFn:
        _faults[name] = fn
        return fn

    return decorator


def list_faults() -> list[str]:
    return sorted(_faults)


def apply_fault(name: str, inputs: Inputs) -> Inputs:
    if name not in _faults:
        raise KeyError(f"Unknown fault {name!r}. Known: {list_faults()}")
    return _faults[name](inputs)


@fault("null_required_columns")
def null_required_columns(inputs: Inputs) -> Inputs:
    """Set every non-nullable string column on every input to NULL.

    Exercises validation logic: a pipeline that didn't filter NULLs before
    a NOT-NULL output column will surface as a verify_with_model failure.
    """
    sources = type(inputs).sources()
    new_dataframes = {}
    for name in sources:
        df = getattr(inputs, name)
        new_df = df
        for field in df.schema.fields:
            if field.dataType.simpleString() == "string" and not field.nullable:
                new_df = new_df.withColumn(field.name, f.lit(None).cast("string"))
        new_dataframes[name] = new_df
    return type(inputs).from_dataframes(new_dataframes)


@fault("duplicate_keys")
def duplicate_keys(inputs: Inputs) -> Inputs:
    """Double every row in every input.

    Exposes pipelines that assume primary-key uniqueness without explicit
    dedup — they will produce 2x output rows.
    """
    sources = type(inputs).sources()
    new_dataframes = {
        name: getattr(inputs, name).unionByName(getattr(inputs, name))
        for name in sources
    }
    return type(inputs).from_dataframes(new_dataframes)


@fault("empty_inputs")
def empty_inputs(inputs: Inputs) -> Inputs:
    """Replace every input with an empty DataFrame matching its schema.

    Smoke-tests pipelines for the "what happens with no upstream rows"
    case — a common cause of NULL aggregation surprises.
    """
    sources = type(inputs).sources()
    new_dataframes = {name: getattr(inputs, name).limit(0) for name in sources}
    return type(inputs).from_dataframes(new_dataframes)


__all__ = ["apply_fault", "fault", "list_faults"]
