#!/usr/bin/env python3
"""
Food Knowledge Graph Application

This script provides a command-line interface for building and interacting with
a knowledge graph of food data, recipes, dietary preferences, and personalized
recommendations using Neo4j as the graph database.

Usage:
    python main.py [options]

Example:
    python main.py --data-dir data --start-docker
"""

import os
import argparse
import logging
import sys
import time
from typing import Dict, Any, List

import pandas as pd
import json

from src.graph_db.neo4j.connection import (
    Neo4jConnection,
    start_neo4j_docker,
    stop_neo4j_docker,
)
from src.graph_db.schema.definition import KnowledgeGraphSchema
from src.graph_db.loaders.food_items import FoodItemLoader
from src.graph_db.loaders.recipes import RecipeLoader
from src.graph_db.loaders.persons import PersonLoader
from src.graph_db.relationships import RelationshipBuilder
from src.graph_db.queries.manager import QueryManager
from src.utils.ingredient_parser import split_ingredients, parse_ingredient, parse_ingredient_only_ingredient


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger("food_kg")


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
        logger.info("Starting Neo4j Docker container...")
        return start_neo4j_docker(compose_file=self.compose_file)

    def stop_docker(self) -> bool:
        """
        Stop the Neo4j Docker container.

        Returns:
            bool: True if successful, False otherwise
        """
        logger.info("Stopping Neo4j Docker container...")
        return stop_neo4j_docker(compose_file=self.compose_file)

    def connect(self) -> bool:
        """
        Connect to the Neo4j database and initialize all components.

        Returns:
            bool: True if connection successful, False otherwise
        """
        logger.info(f"Connecting to Neo4j at {self.uri}...")
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
        logger.info("Setting up schema (constraints and indexes)...")
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

        # Load food data
        logger.info("Loading food data...")
        food_data_path = os.path.join(data_dir, "food_data.csv")
        if os.path.exists(food_data_path):
            food_df = pd.read_csv(food_data_path)
            results["food_items"] = self.food_loader.load_data(food_df)
        else:
            logger.error(f"Food data file not found: {food_data_path}")
            results["food_items"] = {"status": "error", "error": "File not found"}

        # Load recipe data from full_format_recipes.json
        logger.info("Loading recipes from full_format_recipes.json...")
        recipes_json_path = os.path.join(data_dir, "full_format_recipes.json")
        if os.path.exists(recipes_json_path):
            try:
                with open(recipes_json_path, "r") as f:
                    recipes_data = json.load(f)
                recipes_df = pd.DataFrame(recipes_data)
                results["recipes_json"] = self.recipe_loader.load_data(
                    recipes_df, "full_format_recipes", sample_size=sample_recipes
                )
            except Exception as e:
                logger.error(f"Error loading recipes JSON: {e}")
                results["recipes_json"] = {"status": "error", "error": str(e)}
        else:
            logger.error(f"Recipes JSON file not found: {recipes_json_path}")
            results["recipes_json"] = {"status": "error", "error": "File not found"}

        # Load recipe data from recipes.parquet
        logger.info("Loading recipes from recipes.parquet...")
        recipes_parquet_path = os.path.join(data_dir, "recipes.parquet")
        if os.path.exists(recipes_parquet_path):
            try:
                recipes_df = pd.read_parquet(recipes_parquet_path)
                results["recipes_parquet"] = self.recipe_loader.load_data(
                    recipes_df, "recipes_parquet", sample_size=sample_recipes
                )
            except Exception as e:
                logger.error(f"Error loading recipes parquet: {e}")
                results["recipes_parquet"] = {"status": "error", "error": str(e)}
        else:
            logger.error(f"Recipes parquet file not found: {recipes_parquet_path}")
            results["recipes_parquet"] = {"status": "error", "error": "File not found"}

        # Load person data
        logger.info("Loading person data...")
        persons_path = os.path.join(data_dir, "personalized_diet_recommendations.csv")
        if os.path.exists(persons_path):
            try:
                persons_df = pd.read_csv(persons_path)
                results["persons"] = self.person_loader.load_data(
                    persons_df, sample_size=sample_persons
                )
            except Exception as e:
                logger.error(f"Error loading persons data: {e}")
                results["persons"] = {"status": "error", "error": str(e)}
        else:
            logger.error(f"Persons data file not found: {persons_path}")
            results["persons"] = {"status": "error", "error": "File not found"}

        return results

    def create_relationships(self) -> Dict[str, Any]:
        """
        Create relationships between entities in the graph.

        Returns:
            Dict with relationship creation results
        """
        logger.info("Creating relationships between entities...")
        return self.relationship_builder.create_relationships()

    def run_queries(self) -> Dict[str, pd.DataFrame]:
        """
        Run predefined queries on the knowledge graph.

        Returns:
            Dict with query results as DataFrames
        """
        logger.info("Running queries to verify data...")

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


def parse_args():
    """Parse command line arguments with descriptions."""
    parser = argparse.ArgumentParser(
        description="Food Knowledge Graph Manager",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    # Data source parameters
    parser.add_argument(
        "--data-dir", 
        default="data", 
        help="Directory containing data files"
    )
    
    # Connection parameters
    parser.add_argument(
        "--docker-compose",
        default="docker-compose.yml",
        help="Path to docker-compose.yml file",
    )
    parser.add_argument(
        "--uri", 
        default="bolt://localhost:7687", 
        help="Neo4j connection URI"
    )
    parser.add_argument(
        "--user", 
        default="neo4j", 
        help="Neo4j username"
    )
    parser.add_argument(
        "--password", 
        default="password", 
        help="Neo4j password"
    )
    
    # Docker control parameters
    parser.add_argument(
        "--start-docker", 
        action="store_true", 
        help="Start Neo4j Docker container"
    )
    parser.add_argument(
        "--stop-docker", 
        action="store_true", 
        help="Stop Neo4j Docker container"
    )
    
    # Data sampling parameters
    parser.add_argument(
        "--sample-recipes", 
        type=int, 
        default=10000, 
        help="Number of recipes to sample"
    )
    parser.add_argument(
        "--sample-persons", 
        type=int, 
        default=10000, 
        help="Number of persons to sample"
    )
    
    # Skip-step parameters
    parser.add_argument(
        "--skip-schema", 
        action="store_true", 
        help="Skip schema setup"
    )
    parser.add_argument(
        "--skip-food", 
        action="store_true", 
        help="Skip food data loading"
    )
    parser.add_argument(
        "--skip-recipes", 
        action="store_true", 
        help="Skip recipe data loading"
    )
    parser.add_argument(
        "--skip-persons", 
        action="store_true", 
        help="Skip person data loading"
    )
    parser.add_argument(
        "--skip-relationships", 
        action="store_true", 
        help="Skip relationship creation"
    )
    parser.add_argument(
        "--skip-queries", 
        action="store_true", 
        help="Skip running verification queries"
    )

    return parser.parse_args()


def main():
    """
    Main entry point for the food knowledge graph manager.
    
    Orchestrates the entire process of setting up and querying the knowledge graph
    based on command-line arguments.
    """
    args = parse_args()

    # Create the knowledge graph manager
    kg = FoodKnowledgeGraph(
        uri=args.uri,
        user=args.user,
        password=args.password,
        compose_file=args.docker_compose,
    )

    try:
        # Start Docker container if requested
        if args.start_docker:
            if not kg.start_docker():
                logger.error("Failed to start Docker container")
                return 1
            logger.info("Waiting for Neo4j to start up...")
            time.sleep(10)  # Wait for container to fully start

        # Connect to Neo4j
        if not kg.connect():
            logger.error("Failed to connect to Neo4j")
            return 1

        # Set up schema
        if not args.skip_schema:
            schema_results = kg.setup_schema()
            logger.info(f"Schema setup results: {json.dumps(schema_results, indent=2)}")

        # Load data
        if not args.skip_food or not args.skip_recipes or not args.skip_persons:
            kg.load_data(
                args.data_dir,
                sample_recipes=args.sample_recipes,
                sample_persons=args.sample_persons
            )
            logger.info("Data loading completed")

        # Create relationships
        if not args.skip_relationships:
            rel_results = kg.create_relationships()
            logger.info(
                f"Relationship creation results: {json.dumps(rel_results, indent=2)}"
            )

        # Run queries
        if not args.skip_queries:
            query_results = kg.run_queries()
            logger.info("Query results:")
            for name, df in query_results.items():
                logger.info(f"--- {name} ---")
                if not df.empty:
                    logger.info(f"\n{df}")

        # Print visualization queries
        vis_queries = kg.get_visualization_queries()
        logger.info(
            "\nVisualization queries for Neo4j Browser (http://localhost:7474):"
        )
        for i, query in enumerate(vis_queries):
            logger.info(f"\n{i + 1}. {query['title']}:")
            logger.info(f"{query['query']}")

        logger.info("\nKnowledge graph setup completed successfully!")

        # Print prominent message about Neo4j Browser access
        print_browser_access_info(args.user, args.password)

    except Exception as e:
        logger.error(f"Error: {e}")
        return 1

    finally:
        # Close connection
        kg.close()

        # Stop Docker container if requested
        if args.stop_docker:
            kg.stop_docker()

    return 0


def print_browser_access_info(user: str, password: str) -> None:
    """
    Print information about how to access the Neo4j Browser.
    
    Args:
        user: Neo4j username
        password: Neo4j password
    """
    print("\n" + "=" * 80)
    print("  NEO4J BROWSER ACCESS")
    print("=" * 80)
    print("  You can access your Knowledge Graph at: http://localhost:7474/")
    print(f"  Username: {user}")
    print(f"  Password: {password}")
    print("=" * 80)
    print("  To explore your graph, try these Cypher queries:")
    print("  MATCH (n) RETURN n LIMIT 25")
    print(
        "  MATCH (p:Person)-[:RECOMMENDED_RECIPE]->(r:Recipe) RETURN p, r LIMIT 10"
    )
    print("=" * 80)


if __name__ == "__main__":
    sys.exit(main())
