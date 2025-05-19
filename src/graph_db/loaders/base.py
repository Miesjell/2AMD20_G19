"""
Base loader module for the food knowledge graph.
"""

from abc import ABC, abstractmethod
from typing import Optional, Dict, List, Any

import pandas as pd
import numpy as np
from neo4j import Driver


class DataLoader(ABC):
    """Abstract base class for data loaders."""

    def __init__(self, driver: Optional[Driver] = None):
        """
        Initialize the data loader.

        Args:
            driver: Neo4j driver instance
        """
        self.driver = driver

    def set_driver(self, driver: Driver) -> None:
        """
        Set the Neo4j driver.

        Args:
            driver: Neo4j driver instance
        """
        self.driver = driver

    @abstractmethod
    def load_data(self, data: Any, **kwargs) -> Dict[str, Any]:
        """
        Load data into the Neo4j database.

        Args:
            data: The data to load
            **kwargs: Additional options for loading

        Returns:
            Dict with load results
        """
        pass

    def clean_text(self, text: Any) -> Optional[str]:
        """
        Clean text for Neo4j query parameters.

        Args:
            text: Text to clean

        Returns:
            Cleaned text or None
        """
        # Handle None values
        if text is None:
            return None

        # Handle pandas Series
        if isinstance(text, pd.Series):
            return text.apply(self.clean_text)

        # Handle numpy arrays by converting to list
        if isinstance(text, np.ndarray):
            return str(text.tolist()).replace('"', "").replace("'", "").strip()

        # Handle NaN values
        if pd.api.types.is_scalar(text) and pd.isna(text):
            return None

        # Handle numeric types
        if isinstance(text, (int, float)):
            return text

        # Handle lists by converting to string
        if isinstance(text, list):
            return str(text).replace('"', "").replace("'", "").strip()

        # Default string cleaning
        return str(text).replace('"', "").replace("'", "").strip()

    def batch_data(
        self, data: pd.DataFrame, batch_size: int = 50
    ) -> List[pd.DataFrame]:
        """
        Split data into batches for processing.

        Args:
            data: DataFrame to batch
            batch_size: Size of each batch

        Returns:
            List of DataFrame batches
        """
        return [data[i : i + batch_size] for i in range(0, len(data), batch_size)]
