"""Cached SparkSession factory for the Streamlit app."""

from __future__ import annotations

import streamlit as st
from pyspark.sql import SparkSession

from utils.spark_local import build_local_spark


@st.cache_resource
def get_spark() -> SparkSession:
    """Return a process-wide local SparkSession.

    Reuses `utils.spark_local.build_local_spark` so the app, the test
    suite, and the verify CLI share the same configuration.
    """
    return build_local_spark("poorbricks-streamlit")
