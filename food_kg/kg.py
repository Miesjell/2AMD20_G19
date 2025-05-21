import os
import json
import time
import logging
from typing import Dict, Any, List

import pandas as pd

# Neo4j connection
from src.graph_db.neo4j.connection import Neo4jConnection, start_neo4j_docker, stop_neo4j_docker

# Schema and components
from src.graph_db.schema.definition import KnowledgeGraphSchema
from src.graph_db.loaders.food_items import FoodItemLoader
from src.graph_db.loaders.recipes import RecipeLoader
from src.graph_db.loaders.persons import PersonLoader
from src.graph_db.relationships import RelationshipBuilder
from src.graph_db.queries.manager import QueryManager


class FoodKnowledgeGraph:
    """
    Main class for managing the food knowledge graph.
    
    This class serves as the central coordinator for all interactions with the 
    knowledge graph, including data loading, schema setup, relationship creation,
    and query execution.
    """

    def __init__(
        self,
        uri: str = "bolt://localhost:7687",
        user: str = "neo4j",
        password: str = "password",
        compose_file: str = "docker-compose.yml",
        logger: logging.Logger | None = None,
    ):
        """
        Initialize the food knowledge graph manager.

        Args:
            uri: Neo4j connection URI
            user: Neo4j username
            password: Neo4j password
            compose_file: Path to docker-compose.yml file
        """
        # Connection settings
        self.uri = uri
        self.user = user
        self.password = password
        self.compose_file = compose_file
        self.logger = logger or logging.getLogger(__name__)
        self.connection = Neo4jConnection(uri, user, password)
        
        # Component initialization
        self.schema = KnowledgeGraphSchema()
        self.food_loader = FoodItemLoader()
        self.recipe_loader = RecipeLoader()
        self.person_loader = PersonLoader()
        self.relationship_builder = RelationshipBuilder()
        self.query_manager = QueryManager()
        self.driver = None

    def start_docker(self) -> bool:
        """
        Start the Neo4j Docker container.

        Returns:
            bool: True if successful, False otherwise
        """
        self.logger.info("Starting Neo4j Docker container...")
        return start_neo4j_docker(compose_file=self.compose_file)

    def stop_docker(self) -> bool:
        """
        Stop the Neo4j Docker container.

        Returns:
            bool: True if successful, False otherwise
        """
        self.logger.info("Stopping Neo4j Docker container...")
        return stop_neo4j_docker(compose_file=self.compose_file)

    def connect(self) -> bool:
        """
        Connect to the Neo4j database and initialize all components.

        Returns:
            bool: True if connection successful, False otherwise
        """
        self.logger.info(f"Connecting to Neo4j at {self.uri}...")
        if self.connection.connect():
            self.driver = self.connection.get_driver()

            # Set driver in all components
            self.schema.set_driver(self.driver)
            self.food_loader.set_driver(self.driver)
            self.recipe_loader.set_driver(self.driver)
            self.person_loader.set_driver(self.driver)
            self.relationship_builder.set_driver(self.driver)
            self.query_manager.set_driver(self.driver)

            return True
        return False

    def close(self) -> None:
        """Close the Neo4j connection."""
        if self.connection:
            self.connection.close()

    def setup_schema(self) -> Dict[str, Any]:
        """
        Set up the Neo4j schema with constraints and indexes.

        Returns:
            Dict with setup results
        """
        self.logger.info("Setting up schema (constraints and indexes)...")
        return self.schema.setup_schema()

    def load_data(
        self, data_dir: str, sample_recipes: int = 1000, sample_persons: int = 1000
    ) -> Dict[str, Any]:
        """
        Load all data into the knowledge graph.

        Args:
            data_dir: Directory containing data files
            sample_recipes: Number of recipes to sample (to avoid memory issues)
            sample_persons: Number of persons to sample

        Returns:
            Dict with load results
        """
        results = {}

        def try_load_file(path, loader_fn, key, **kwargs):
            if os.path.exists(path):
                try:
                    if path.endswith(".json"):
                        with open(path, "r") as f:
                            df = pd.DataFrame(json.load(f))
                    elif path.endswith(".parquet"):
                        df = pd.read_parquet(path)
                    elif path.endswith(".csv"):
                        df = pd.read_csv(path)
                    else:
                        raise ValueError(f"Unsupported file type: {path}")
                    results[key] = loader_fn(df, **kwargs)
                except Exception as e:
                    self.logger.error(f"Error loading {key} from {path}: {e}")
                    results[key] = {"status": "error", "error": str(e)}
            else:
                self.logger.error(f"{key.capitalize()} file not found: {path}")
                results[key] = {"status": "error", "error": "File not found"}

        self.logger.info("Loading food data...")
        try_load_file(
            os.path.join(data_dir, "food_data.csv"),
            self.food_loader.load_data,
            "food_items"
        )

        self.logger.info("Loading recipes from full_format_recipes.json...")
        try_load_file(
            os.path.join(data_dir, "full_format_recipes.json"),
            self.recipe_loader.load_data,
            "recipes_json",
            source_name="full_format_recipes",
            sample_size=sample_recipes
        )

        self.logger.info("Loading recipes from recipes.parquet...")
        try_load_file(
            os.path.join(data_dir, "recipes.parquet"),
            self.recipe_loader.load_data,
            "recipes_parquet",
            source_name="recipes_parquet",
            sample_size=sample_recipes
        )

        self.logger.info("Loading person data...")
        try_load_file(
            os.path.join(data_dir, "personalized_diet_recommendations.csv"),
            self.person_loader.load_data,
            "persons",
            sample_size=sample_persons
        )

        return results

    def create_relationships(self) -> Dict[str, Any]:
        """
        Create relationships between entities in the graph.

        Returns:
            Dict with relationship creation results
        """
        self.logger.info("Creating relationships between entities...")
        return self.relationship_builder.create_relationships()

    def run_queries(self) -> Dict[str, pd.DataFrame]:
        """
        Run predefined queries on the knowledge graph.

        Returns:
            Dict with query results as DataFrames
        """
        self.logger.info("Running queries to verify data...")

        results = {
            "node_counts": self.query_manager.count_nodes_by_type(),
            "allergens": self.query_manager.find_allergens_and_causes(),
            "diet_preferences": self.query_manager.find_diet_preferences(),
            "recipes": self.query_manager.find_recipes_with_ingredients(),
            "recommendations": self.query_manager.find_recommended_recipes(),
        }

        return results

    def get_visualization_queries(self) -> List[Dict[str, str]]:
        """
        Get visualization queries for Neo4j Browser.

        Returns:
            List of visualization queries
        """
        return self.query_manager.get_visualization_queries()