# ingestion sub-package — lazy imports so missing optional deps (sqlalchemy)
# don't break CSV/API pipelines.

def __getattr__(name):
    if name == "CSVSource":
        from .csv_source import CSVSource
        return CSVSource
    if name == "APISource":
        from .api_source import APISource
        return APISource
    if name == "DBSource":
        from .db_source import DBSource
        return DBSource
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")

__all__ = ["CSVSource", "APISource", "DBSource"]
