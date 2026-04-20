from aqueduct.executor.spark.executor import ExecuteError, execute

_SUPPORTED_ENGINES = ("spark",)


def get_executor(engine: str = "spark"):
    """Return the execute() function for the requested engine.

    Args:
        engine: Execution engine name.  Currently only ``"spark"`` is supported.

    Raises:
        NotImplementedError: Engine is recognised as planned but not yet implemented.
        ValueError: Engine is unknown.
    """
    if engine == "spark":
        from aqueduct.executor.spark.executor import execute as spark_execute
        return spark_execute
    if engine == "flink":
        raise NotImplementedError(
            "Flink engine is planned but not yet implemented. "
            "Set deployment.engine: spark in aqueduct.yml."
        )
    raise ValueError(
        f"Unknown execution engine: {engine!r}. "
        f"Supported: {', '.join(_SUPPORTED_ENGINES)}"
    )


__all__ = ["execute", "ExecuteError", "get_executor"]
