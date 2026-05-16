"""Discovery registries for pipelines and fixture scenarios.

Pipelines self-register when their `pipeline.py` module is imported (the
`@pipeline` decorator does this). Fixture scenarios self-register when
`fixtures.py` is imported and its `@scenario` decorators run.

The runner imports a pipeline module by path (e.g. ``status.aon_monthly_status``)
which transitively imports ``pipeline.py`` and (if present) ``fixtures.py``.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING, Any, TypeVar

if TYPE_CHECKING:
    from .inputs import Inputs

ScenarioFn = Callable[[], "Inputs"]
_F = TypeVar("_F", bound=Callable[..., Any])


_pipelines: dict[str, PipelineMeta] = {}
_scenarios: dict[str, dict[str, ScenarioFn]] = {}


def all_pipelines() -> dict[str, PipelineMeta]:
    """Return a snapshot of every registered pipeline keyed by table name.

    Returns a defensive copy — mutating the result will not affect the registry.
    Used by the runner to resolve a pipeline by its module path.
    """
    return dict(_pipelines)


class PipelineMeta:
    """Records a single registered pipeline.

    Holds the original undecorated function (called by the runner with an
    explicit Inputs instance) and the schema-validated wrapper (used for
    type-safe invocation in tests).
    """

    def __init__(
        self,
        table_name: str,
        original_fn: Callable[..., object],
        validated_fn: Callable[..., object],
        inputs_cls: type[Inputs],
        model: type,
        level: str,
        comment: str,
        module: str,
        target_storage: str = "delta",
    ) -> None:
        self.table_name = table_name
        self.original_fn = original_fn
        self.validated_fn = validated_fn
        self.inputs_cls = inputs_cls
        self.model = model
        self.level = level
        self.comment = comment
        self.module = module
        # "delta" (Spark memory, test/fixture mode) or
        # "postgres" (writes to analytics.<level>.<name> via PostgresLoader).
        self.target_storage = target_storage


def _registry_key(table_name: str, target_storage: str) -> str:
    """Compose a unique registry key.

    Delta and Postgres pipelines can share the same logical table name
    (different stores, same business meaning). Disambiguate by storage.
    """
    return f"{target_storage}:{table_name}"


def register_pipeline(meta: PipelineMeta) -> None:
    key = _registry_key(meta.table_name, meta.target_storage)
    if key in _pipelines:
        existing = _pipelines[key]
        if existing.module != meta.module:
            raise ValueError(
                f"Pipeline {meta.table_name!r} (storage={meta.target_storage}) "
                f"already registered from {existing.module!r}; cannot re-register from "
                f"{meta.module!r}."
            )
        # Same module re-importing itself: idempotent.
    _pipelines[key] = meta


def get_pipeline(
    table_name: str,
    target_storage: str | None = None,
) -> PipelineMeta:
    """Look up a pipeline by table name. If ``target_storage`` is omitted
    and there's only one match across storages, return it; otherwise
    require disambiguation.
    """
    if target_storage is not None:
        key = _registry_key(table_name, target_storage)
        if key not in _pipelines:
            raise KeyError(
                f"Pipeline {table_name!r} (storage={target_storage}) not registered. "
                f"Known: {sorted(_pipelines)}"
            )
        return _pipelines[key]
    matches = [p for p in _pipelines.values() if p.table_name == table_name]
    if not matches:
        raise KeyError(
            f"Pipeline {table_name!r} not registered. Known: {sorted(_pipelines)}"
        )
    if len(matches) > 1:
        descriptors = sorted(f"storage={p.target_storage}" for p in matches)
        raise KeyError(
            f"Pipeline {table_name!r} is ambiguous — registered under "
            f"{descriptors}. Pass target_storage= to disambiguate."
        )
    return matches[0]


def list_pipelines() -> list[str]:
    """Return registry keys (storage:table_name) for all registered pipelines."""
    return sorted(_pipelines)


def scenario(name: str) -> Callable[[_F], _F]:
    """Register a fixture scenario for the calling pipeline.

    Scenario functions live in a pipeline's ``fixtures.py`` and return an
    instance of that pipeline's Inputs subclass. The framework groups
    scenarios by the function's module path: every scenario in
    ``source.pipelines.X.Y.fixtures`` is associated with pipeline ``X.Y``.
    """

    def decorator(fn: _F) -> _F:
        module = fn.__module__
        # Strip the ".fixtures" suffix to get the pipeline module path,
        # then strip the leading "tables." to get the dotted name.
        pipeline_module = module.removesuffix(".fixtures")
        pipeline_key = pipeline_module.removeprefix("tables.")
        _scenarios.setdefault(pipeline_key, {})[name] = fn
        return fn

    return decorator


def list_scenarios(pipeline_key: str) -> dict[str, ScenarioFn]:
    """Return all scenarios for a pipeline, keyed by scenario name."""
    return dict(_scenarios.get(pipeline_key, {}))


__all__ = [
    "PipelineMeta",
    "_registry_key",
    "all_pipelines",
    "get_pipeline",
    "list_pipelines",
    "list_scenarios",
    "register_pipeline",
    "scenario",
]
