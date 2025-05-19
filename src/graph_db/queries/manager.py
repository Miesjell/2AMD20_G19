"""
Query utilities for the food knowledge graph.
"""
from typing import Dict, Any, List, Optional

import pandas as pd
from neo4j import Driver


class QueryManager:
    """Manages predefined queries for the food knowledge graph."""
    
    def __init__(self, driver: Optional[Driver] = None):
        """
        Initialize the query manager.
        
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
    
    def count_nodes_by_type(self) -> pd.DataFrame:
        """
        Count nodes by type in the graph.
        
        Returns:
            DataFrame with node counts by type
        """
        if not self.driver:
            return pd.DataFrame({"Error": ["Neo4j driver not set. Call set_driver() first."]})
        
        query = """
        MATCH (n)
        RETURN labels(n) AS NodeType, count(n) AS Count
        ORDER BY Count DESC
        """
        
        with self.driver.session() as session:
            result = session.run(query)
            records = result.data()
            if not records:
                return pd.DataFrame()
            return pd.DataFrame(records)
    
    def find_allergens_and_causes(self, limit: int = 10) -> pd.DataFrame:
        """
        Find allergens and food items that cause them.
        
        Args:
            limit: Number of results to return
            
        Returns:
            DataFrame with allergens and their causes
        """
        if not self.driver:
            return pd.DataFrame({"Error": ["Neo4j driver not set. Call set_driver() first."]})
        
        query = f"""
        MATCH (a:Allergy)<-[:CAUSES_ALLERGY]-(f:FoodItem)
        RETURN a.name AS Allergen, collect(f.name) AS CausedByFoods, count(f) AS FoodCount
        ORDER BY FoodCount DESC
        LIMIT {limit}
        """
        
        with self.driver.session() as session:
            result = session.run(query)
            records = result.data()
            if not records:
                return pd.DataFrame()
            return pd.DataFrame(records)
    
    def find_diet_preferences(self) -> pd.DataFrame:
        """
        Find popular diet preferences.
        
        Returns:
            DataFrame with diet preferences and follower counts
        """
        if not self.driver:
            return pd.DataFrame({"Error": ["Neo4j driver not set. Call set_driver() first."]})
        
        query = """
        MATCH (p:Person)-[:FOLLOWS]->(d:DietPreference)
        RETURN d.name AS DietPreference, count(p) AS Followers
        ORDER BY Followers DESC
        """
        
        with self.driver.session() as session:
            result = session.run(query)
            records = result.data()
            if not records:
                return pd.DataFrame()
            return pd.DataFrame(records)
    
    def find_recipes_with_ingredients(self, limit: int = 10) -> pd.DataFrame:
        """
        Find recipes with their ingredients.
        
        Args:
            limit: Number of results to return
            
        Returns:
            DataFrame with recipes and their ingredients
        """
        if not self.driver:
            return pd.DataFrame({"Error": ["Neo4j driver not set. Call set_driver() first."]})
        
        query = f"""
        MATCH (r:Recipe)-[:CONTAINS]->(i:Ingredient)
        WITH r, collect(i.name) AS ingredients
        RETURN r.name AS Recipe, r.calories AS Calories, 
               ingredients as Ingredients, size(ingredients) as IngredientCount
        ORDER BY r.calories ASC
        LIMIT {limit}
        """
        
        with self.driver.session() as session:
            result = session.run(query)
            records = result.data()
            if not records:
                return pd.DataFrame()
            return pd.DataFrame(records)
    
    def find_recommended_recipes(self, limit: int = 10) -> pd.DataFrame:
        """
        Find recipe recommendations for people with allergies.
        
        Args:
            limit: Number of results to return
            
        Returns:
            DataFrame with people, their allergies, and recommended recipes
        """
        if not self.driver:
            return pd.DataFrame({"Error": ["Neo4j driver not set. Call set_driver() first."]})
        
        query = f"""
        MATCH (p:Person)-[:HAS_ALLERGY]->(a:Allergy)
        MATCH (p)-[:RECOMMENDED_RECIPE]->(r:Recipe)
        RETURN p.id AS Person, a.name AS Allergy, 
               collect(r.name)[..5] AS RecommendedRecipes, count(r) AS RecipeCount
        ORDER BY RecipeCount DESC
        LIMIT {limit}
        """
        
        with self.driver.session() as session:
            result = session.run(query)
            records = result.data()
            if not records:
                return pd.DataFrame()
            return pd.DataFrame(records)
    
    def get_visualization_queries(self) -> List[Dict[str, str]]:
        """
        Get a list of visualization queries to run in the Neo4j Browser.
        
        Returns:
            List of dictionaries with query titles and Cypher code
        """
        return [
            {
                "title": "Show a small sample of all node types and relationships",
                "query": """
                MATCH (n)
                WITH n LIMIT 25
                OPTIONAL MATCH (n)-[r]-(m)
                RETURN n, r, m
                """
            },
            {
                "title": "Explore the connections between recipes and ingredients",
                "query": """
                MATCH (r:Recipe)-[:CONTAINS]->(i:Ingredient)
                WITH r, collect(i) AS ingredients
                RETURN r, ingredients
                LIMIT 3
                """
            },
            {
                "title": "See the relationships between allergies and prohibited foods",
                "query": """
                MATCH (a:Allergy)-[:PROHIBITS]->(f:FoodItem)
                WHERE a.name CONTAINS 'Nut'
                RETURN a, f
                """
            },
            {
                "title": "View dietary preferences and people who follow them",
                "query": """
                MATCH (p:Person)-[:FOLLOWS]->(d:DietPreference)
                RETURN d, p
                LIMIT 10
                """
            },
            {
                "title": "Explore recipe recommendations for a specific diet",
                "query": """
                MATCH (d:DietPreference {name: 'Vegetarian'})-[:INCLUDES]->(r:Recipe)
                RETURN d, r
                LIMIT 10
                """
            }
        ]