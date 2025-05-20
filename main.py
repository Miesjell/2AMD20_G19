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
from utils.helpers import print_browser_access_info
from utils.helpers import parse_args
from food_kg.kg import FoodKnowledgeGraph

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


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger("food_kg")

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

if __name__ == "__main__":
    sys.exit(main())
