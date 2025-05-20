"""
Query utilities for the food knowledge graph.

This module provides a collection of predefined queries for analyzing and 
visualizing data in the food knowledge graph. It serves as a centralized
place to define and execute common queries.
"""
from typing import Dict, Any, List, Optional

import pandas as pd
from neo4j import Driver


class QueryManager:
    """
    Manages predefined queries for the food knowledge graph.
    
    This class provides methods for executing common analytical queries
    and retrieving visualization queries for the Neo4j browser.
    """
    
    def __init__(self, driver: Optional[Driver] = None):
        """
        Initialize the query manager.
        
        Args:
            driver: Neo4j driver instance (optional)
        """
        self.driver = driver
    
    def set_driver(self, driver: Driver) -> None:
        """
        Set the Neo4j driver.
        
        Args:
            driver: Neo4j driver instance
        """
        self.driver = driver
    
    def _execute_query(self, query: str, params: Dict[str, Any] = None) -> pd.DataFrame:
        """
        Execute a query and return results as a DataFrame.
        
        Args:
            query: Cypher query string
            params: Query parameters (optional)
            
        Returns:
            DataFrame with query results
        """
        if not self.driver:
            return pd.DataFrame({"Error": ["Neo4j driver not set. Call set_driver() first."]})
        
        try:
            with self.driver.session() as session:
                result = session.run(query, parameters=params or {})
                records = result.data()
                if not records:
                    return pd.DataFrame()
                return pd.DataFrame(records)
        except Exception as e:
            return pd.DataFrame({"Error": [f"Query execution failed: {str(e)}"]})
    
    def count_nodes_by_type(self) -> pd.DataFrame:
        """
        Count nodes by type in the graph.
        
        Returns:
            DataFrame with node counts by type
        """
        query = """
        MATCH (n)
        RETURN labels(n) AS NodeType, count(n) AS Count
        ORDER BY Count DESC
        """
        return self._execute_query(query)
    
    def find_allergens_and_causes(self, limit: int = 10) -> pd.DataFrame:
        """
        Find allergens and food items that cause them.
        
        Args:
            limit: Number of results to return
            
        Returns:
            DataFrame with allergens and their causes
        """
        query = f"""
        MATCH (a:Allergy)<-[:CAUSES_ALLERGY]-(f:FoodItem)
        RETURN a.name AS Allergen, collect(f.name) AS CausedByFoods, count(f) AS FoodCount
        ORDER BY FoodCount DESC
        LIMIT {limit}
        """
        return self._execute_query(query)
    
    def find_diet_preferences(self) -> pd.DataFrame:
        """
        Find popular diet preferences.
        
        Returns:
            DataFrame with diet preferences and follower counts
        """
        query = """
        MATCH (p:Person)-[:HAS_DIETARY_PREFERENCE]->(d:DietPreference)
        RETURN d.name AS DietPreference, count(p) AS Followers
        ORDER BY Followers DESC
        """
        return self._execute_query(query)
    
    def find_recipes_with_ingredients(self, limit: int = 10) -> pd.DataFrame:
        """
        Find recipes with their ingredients.
        
        Args:
            limit: Number of results to return
            
        Returns:
            DataFrame with recipes and their ingredients
        """
        query = f"""
        MATCH (r:Recipe)-[:CONTAINS]->(i:Ingredient)
        WITH r, collect(i.name) AS ingredients
        RETURN r.name AS Recipe, r.calories AS Calories, 
               ingredients as Ingredients, size(ingredients) as IngredientCount
        ORDER BY r.calories ASC
        LIMIT {limit}
        """
        return self._execute_query(query)
    
    def find_recommended_recipes(self, limit: int = 10) -> pd.DataFrame:
        """
        Find recipe recommendations for people with allergies.
        
        Args:
            limit: Number of results to return
            
        Returns:
            DataFrame with people, their allergies, and recommended recipes
        """
        query = f"""
        MATCH (p:Person)-[:HAS_ALLERGY]->(a:Allergy)
        MATCH (p)-[:RECOMMENDED_RECIPE]->(r:Recipe)
        RETURN p.id AS Person, a.name AS Allergy, 
               collect(r.name)[..5] AS RecommendedRecipes, count(r) AS RecipeCount
        ORDER BY RecipeCount DESC
        LIMIT {limit}
        """
        return self._execute_query(query)
    
    def get_visualization_queries(self) -> List[Dict[str, str]]:
        """
        Get a list of visualization queries to run in the Neo4j Browser.
        
        Returns:
            List of dictionaries with query titles and Cypher code
        """
        return [
            {
                "title": "Overview of graph structure",
                "description": "Show a small sample of all node types and relationships",
                "query": """
                MATCH (n)
                WITH n LIMIT 25
                OPTIONAL MATCH (n)-[r]-(m)
                RETURN n, r, m
                """
            },
            {
                "title": "Recipe ingredients exploration",
                "description": "Explore the connections between recipes and their ingredients",
                "query": """
                MATCH (r:Recipe)-[:CONTAINS]->(i:Ingredient)
                WITH r, collect(i) AS ingredients
                RETURN r, ingredients
                LIMIT 3
                """
            },
            {
                "title": "Allergy-causing foods",
                "description": "See the relationships between nut allergies and prohibited foods",
                "query": """
                MATCH (a:Allergy)-[:PROHIBITS]->(f:FoodItem)
                WHERE a.name CONTAINS 'Nut'
                RETURN a, f
                """
            },
            {
                "title": "Diet preferences",
                "description": "View dietary preferences and people who follow them",
                "query": """
                MATCH (p:Person)-[:HAS_DIETARY_PREFERENCE]->(d:DietPreference)
                RETURN d, p
                LIMIT 10
                """
            },
            {
                "title": "Vegetarian recipes",
                "description": "Explore recipes recommended for a vegetarian diet",
                "query": """
                MATCH (d:DietPreference {name: 'Vegetarian'})-[:INCLUDES]->(r:Recipe)
                RETURN d, r
                LIMIT 10
                """
            },
            {
                "title": "Focused recipe recommendations",
                "description": "Show recipes with at most 3 people per recipe who received recommendations",
                "query": """
                MATCH (r:Recipe)<-[rec:RECOMMENDED_RECIPE]-(p:Person)
                WITH r, p, rec
                WITH r, collect({person: p, recommendation: rec})[0..3] AS peopleWithRecs
                WHERE size(peopleWithRecs) >= 1
                UNWIND peopleWithRecs as personRec
                RETURN r, personRec.recommendation, personRec.person
                LIMIT 100
                """
            }
        ]
    
    def find_popular_ingredients(self, limit: int = 20) -> pd.DataFrame:
        """
        Find the most commonly used ingredients in recipes.
        
        Args:
            limit: Number of results to return
            
        Returns:
            DataFrame with ingredients and their usage counts
        """
        query = f"""
        MATCH (i:Ingredient)<-[:CONTAINS]-(r:Recipe)
        RETURN i.name AS Ingredient, count(r) AS RecipeCount
        ORDER BY RecipeCount DESC
        LIMIT {limit}
        """
        return self._execute_query(query)
    
    def find_allergen_free_recipes(self, allergen_name: str, limit: int = 10) -> pd.DataFrame:
        """
        Find recipes that don't contain a specific allergen.
        
        Args:
            allergen_name: Name of the allergen to avoid
            limit: Number of results to return
            
        Returns:
            DataFrame with allergen-free recipes
        """
        query = f"""
        MATCH (r:Recipe)
        WHERE NOT EXISTS {{
            MATCH (r)-[:MAY_CONTAIN_ALLERGEN]->(:Allergy {{name: $allergen_name}})
        }}
        RETURN r.name AS Recipe, r.calories AS Calories,
               r.preparation_description AS Preparation
        LIMIT {limit}
        """
        return self._execute_query(query, {"allergen_name": allergen_name})
