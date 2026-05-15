"""Date utility functions for PySpark operations."""

from pyspark.sql import Column
from pyspark.sql import functions as f

_MIN_VALID_YEAR: int = 1920


def build_event_date_from_struct(date_field: str, created_at_field: str) -> Column:
    """Build an event date from a Mongo date struct field, with robust fallback rules.

    - year: null or < _MIN_VALID_YEAR → fall back to created_at date
    - month: null or 0 (legacy sentinel for unknown) → coalesced to 1
    - day: null or 0 (legacy sentinel for unknown) → coalesced to 1
    """
    year = f.col(f"{date_field}.year")
    month = f.col(f"{date_field}.month")
    day = f.col(f"{date_field}.day")

    safe_month = f.when(month.isNull() | (month == 0), f.lit(1)).otherwise(month)
    safe_day = f.when(day.isNull() | (day == 0), f.lit(1)).otherwise(day)

    return f.coalesce(
        f.when(
            year.isNotNull() & (year >= _MIN_VALID_YEAR),
            f.make_date(year, safe_month, safe_day),
        ),
        f.to_date(f.col(created_at_field)),
    )


def date_trunc_week_sunday(date_col: Column) -> Column:
    """
    Trunca data para o início da semana (domingo 00:00:00).
    Padrão brasileiro: semana começa no domingo.

    O Spark date_trunc("week") retorna segunda-feira por padrão (ISO 8601).
    Esta função ajusta para retornar domingo, que é o início da semana no Brasil.

    Args:
        date_col: Coluna de data/timestamp

    Returns:
        Coluna com data truncada para domingo da semana (como timestamp)

    Example:
        >>> df.withColumn("week", date_trunc_week_sunday(col("created_at")))
        # Se created_at = 2024-01-15 (segunda), retorna 2024-01-14 00:00:00 (domingo)
        # Se created_at = 2024-01-14 (domingo), retorna 2024-01-14 00:00:00 (domingo)
    """
    # Spark date_trunc("week") retorna segunda-feira
    # Para obter domingo: adiciona 1 dia, trunca para semana (próxima segunda), subtrai 1 dia
    # Usa to_timestamp para garantir que o resultado seja timestamp, não date
    return f.to_timestamp(f.date_sub(f.date_trunc("week", f.date_add(date_col, 1)), 1))
