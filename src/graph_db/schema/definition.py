"""
Schema definition module for the food knowledge graph.

This module defines the schema (node labels, relationship types, constraints,
and indexes) for the Neo4j food knowledge graph.
"""
from typing import List, Dict, Any, Optional

from neo4j import Driver


class KnowledgeGraphSchema:
    """Class to manage the schema for the food knowledge graph."""
    
    def __init__(self, driver: Optional[Driver] = None):
        """
        Initialize the schema manager.
        
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
    
    def create_constraints(self) -> List[Dict[str, Any]]:
        """
        Create uniqueness constraints for the knowledge graph nodes.
        
        Returns:
            List of dictionaries with constraint results
        """
        if not self.driver:
            raise ValueError("Neo4j driver not set. Call set_driver() first.")
        
        constraints = [
            ("CREATE CONSTRAINT IF NOT EXISTS FOR (f:FoodItem) REQUIRE f.name IS UNIQUE",
             "FoodItem.name uniqueness constraint"),
            ("CREATE CONSTRAINT IF NOT EXISTS FOR (r:Recipe) REQUIRE r.id IS UNIQUE",
             "Recipe.id uniqueness constraint"),
            ("CREATE CONSTRAINT IF NOT EXISTS FOR (p:Person) REQUIRE p.id IS UNIQUE",
             "Person.id uniqueness constraint"),
            ("CREATE CONSTRAINT IF NOT EXISTS FOR (a:Allergy) REQUIRE a.name IS UNIQUE",
             "Allergy.name uniqueness constraint"),
            ("CREATE CONSTRAINT IF NOT EXISTS FOR (d:DietPreference) REQUIRE d.name IS UNIQUE",
             "DietPreference.name uniqueness constraint")
        ]
        
        results = []
        
        with self.driver.session() as session:
            for constraint_query, description in constraints:
                try:
                    session.run(constraint_query)
                    results.append({
                        "constraint": description,
                        "status": "created",
                        "error": None
                    })
                except Exception as e:
                    results.append({
                        "constraint": description,
                        "status": "failed",
                        "error": str(e)
                    })
        
        return results

    def create_indexes(self) -> List[Dict[str, Any]]:
        """
        Create indexes for better query performance.
        
        Returns:
            List of dictionaries with index results
        """
        if not self.driver:
            raise ValueError("Neo4j driver not set. Call set_driver() first.")
        
        indexes = [
            ("CREATE INDEX IF NOT EXISTS FOR (f:FoodItem) ON (f.name)",
             "FoodItem.name index"),
            ("CREATE INDEX IF NOT EXISTS FOR (r:Recipe) ON (r.name)",
             "Recipe.name index"),
            ("CREATE INDEX IF NOT EXISTS FOR (a:Allergy) ON (a.name)",
             "Allergy.name index"),
        ]
        
        results = []
        
        with self.driver.session() as session:
            for index_query, description in indexes:
                try:
                    session.run(index_query)
                    results.append({
                        "index": description,
                        "status": "created",
                        "error": None
                    })
                except Exception as e:
                    results.append({
                        "index": description,
                        "status": "failed",
                        "error": str(e)
                    })
        
        return results
    
    def setup_schema(self) -> Dict[str, List[Dict[str, Any]]]:
        """
        Set up the complete schema (constraints and indexes).
        
        Returns:
            Dictionary with results of schema creation
        """
        constraint_results = self.create_constraints()
        index_results = self.create_indexes()
        
        return {
            "constraints": constraint_results,
            "indexes": index_results
        }
    
    def check_schema(self) -> Dict[str, Any]:
        """
        Check the existing schema in the database.
        
        Returns:
            Dictionary with information about constraints and indexes
        """
        if not self.driver:
            raise ValueError("Neo4j driver not set. Call set_driver() first.")
        
        schema_info = {
            "constraints": [],
            "indexes": []
        }
        
        with self.driver.session() as session:
            # Check constraints
            result = session.run("SHOW CONSTRAINTS")
            for record in result:
                schema_info["constraints"].append({
                    "name": record.get("name"),
                    "type": record.get("type"),
                    "entityType": record.get("entityType"),
                    "labelsOrTypes": record.get("labelsOrTypes"),
                    "properties": record.get("properties"),
                })
            
            # Check indexes
            result = session.run("SHOW INDEXES")
            for record in result:
                schema_info["indexes"].append({
                    "name": record.get("name"),
                    "type": record.get("type"),
                    "entityType": record.get("entityType"),
                    "labelsOrTypes": record.get("labelsOrTypes"),
                    "properties": record.get("properties"),
                })
        
        return schema_info