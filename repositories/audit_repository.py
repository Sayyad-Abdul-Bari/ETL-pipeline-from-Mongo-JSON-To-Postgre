from abc import ABC, abstractmethod

import pandas as pd


class AuditRepository(ABC):
    @property
    @abstractmethod
    def cache_key(self) -> str:
        """Return a cache key for Streamlit caching."""

    @abstractmethod
    def fetch_ingestion_audit(self) -> pd.DataFrame:
        """Fetch ingestion audit data."""

    @abstractmethod
    def fetch_missing_columns_report(self) -> pd.DataFrame:
        """Fetch missing columns report data."""

    @abstractmethod
    def fetch_missing_collections_report(self) -> pd.DataFrame:
        """Fetch missing collections report data."""
