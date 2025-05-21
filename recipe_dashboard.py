"""
Recipe Recommendation Dashboard

This Streamlit application provides personalized recipe recommendations based on
a user's dietary preferences and allergies. It connects to a Neo4j graph database
containing recipe data, ingredients, allergies, and diet preferences.
"""

import streamlit as st
import pandas as pd
import numpy as np
from typing import List, Dict, Any, Optional

# Import Neo4j connection modules from the existing project
from src.graph_db.neo4j.connection import Neo4jConnection
from src.graph_db.queries.manager import QueryManager

# Set page config
st.set_page_config(
    page_title="Recipe Recommendations",
    page_icon="🍲",
    layout="wide",
)

# Create a class for our recipe recommendation dashboard
class RecipeRecommendationDashboard:
    """
    A Streamlit dashboard that provides personalized recipe recommendations
    based on user dietary preferences and allergies.
    """
    
    def __init__(self):
        """Initialize the dashboard with database connection."""
        # Initialize session state variables if they don't exist
        if 'connected' not in st.session_state:
            st.session_state.connected = False
        if 'connection' not in st.session_state:
            st.session_state.connection = None
        if 'query_manager' not in st.session_state:
            st.session_state.query_manager = None
        if 'diet_preferences' not in st.session_state:
            st.session_state.diet_preferences = []
        if 'allergies' not in st.session_state:
            st.session_state.allergies = []
        if 'favorite_recipes' not in st.session_state:
            st.session_state.favorite_recipes = []
        if 'user_id' not in st.session_state:
            # Generate a simple user ID for the session
            st.session_state.user_id = f"user_{np.random.randint(10000)}"
        
    @property
    def connected(self):
        """Get connection status from session state."""
        return st.session_state.connected
    
    @property
    def connection(self):
        """Get connection from session state."""
        return st.session_state.connection
    
    @property
    def query_manager(self):
        """Get query manager from session state."""
        return st.session_state.query_manager
    
    @property
    def diet_preferences(self):
        """Get diet preferences from session state."""
        return st.session_state.diet_preferences
    
    @property
    def allergies(self):
        """Get allergies from session state."""
        return st.session_state.allergies
        
    def connect_to_database(self):
        """Establish connection to the Neo4j database."""
        # Skip if already connected
        if st.session_state.connected:
            return True
            
        try:
            connection = Neo4jConnection(
                uri="bolt://localhost:7687",
                user="neo4j",
                password="password"
            )
            if connection.connect():
                st.session_state.connection = connection
                st.session_state.query_manager = QueryManager(connection.get_driver())
                st.session_state.connected = True
                
                # Load diet preferences and allergies
                self._load_diet_preferences()
                self._load_allergies()
                return True
            return False
        except Exception as e:
            st.error(f"Failed to connect to database: {e}")
            return False
    
    def _load_diet_preferences(self):
        """Load available diet preferences from the database."""
        if not st.session_state.connected:
            return
        
        try:
            # Use direct query that matches your database schema with INCLUDES/EXCLUDES relationships
            query = """
            MATCH (d:DietPreference)
            RETURN DISTINCT d.name AS DietPreference
            ORDER BY d.name
            """
            
            df = st.session_state.connection.execute_query_to_df(query, {})
            
            if df.empty:
                # Fall back to hardcoded common diet preferences
                st.session_state.diet_preferences = [
                    "Vegetarian", "Vegan", "Pescatarian", 
                    "Gluten-Free", "Dairy-Free", "Low-Carb",
                    "Keto", "Paleo", "Mediterranean"
                ]
                st.sidebar.warning("No diet preferences found in database, using defaults")
            else:
                st.session_state.diet_preferences = df["DietPreference"].tolist()
                
        except Exception as e:
            st.error(f"Error loading diet preferences: {e}")
            # Fallback to common diet preferences
            st.session_state.diet_preferences = [
                "Vegetarian", "Vegan", "Pescatarian", 
                "Gluten-Free", "Dairy-Free", "Low-Carb",
                "Keto", "Paleo", "Mediterranean"
            ]
    
    def _load_allergies(self):
        """Load available allergies from the database."""
        if not st.session_state.connected:
            return
        
        try:
            # Use direct query to match your database schema
            query = """
            MATCH (a:Allergy)
            RETURN DISTINCT a.name AS Allergen
            ORDER BY a.name
            LIMIT 50
            """
            
            df = st.session_state.connection.execute_query_to_df(query, {})
            
            if df.empty:
                # Fallback to common allergies
                st.session_state.allergies = [
                    "Peanuts", "Tree Nuts", "Milk", "Eggs", "Fish",
                    "Shellfish", "Soy", "Wheat", "Gluten", "Sesame"
                ]
                st.sidebar.warning("No allergies found in database, using defaults")
            else:
                st.session_state.allergies = df["Allergen"].tolist()
        
        except Exception as e:
            st.error(f"Error loading allergies: {e}")
            # Fallback to common allergies
            st.session_state.allergies = [
                "Peanuts", "Tree Nuts", "Milk", "Eggs", "Fish",
                "Shellfish", "Soy", "Wheat", "Gluten", "Sesame"
            ]
    
    def find_recipes_for_diet(self, diet_preference: str, limit: int = 10) -> pd.DataFrame:
        """Find recipes suitable for a specific diet preference."""
        if not st.session_state.connected:
            return pd.DataFrame()
        
        query = f"""
        MATCH (d:DietPreference {{name: $diet_name}})-[:INCLUDES]->(r:Recipe)
        RETURN r.name AS Recipe, r.calories AS Calories, 
               r.preparation_description AS Preparation
        LIMIT {limit}
        """
        
        return st.session_state.connection.execute_query_to_df(query, {"diet_name": diet_preference})
    
    def find_recipes_without_allergens(self, allergens: List[str], limit: int = 10) -> pd.DataFrame:
        """Find recipes that don't contain specific allergens."""
        if not st.session_state.connected or not allergens:
            return pd.DataFrame()
        
        allergen_params = {f"allergen{i}": allergen for i, allergen in enumerate(allergens)}
        allergen_where_conditions = [
            f"NOT EXISTS {{ MATCH (r)-[:MAY_CONTAIN_ALLERGEN]->(:Allergy {{name: $allergen{i}}}) }}"
            for i in range(len(allergens))
        ]
        
        query = f"""
        MATCH (r:Recipe)
        WHERE {" AND ".join(allergen_where_conditions)}
        RETURN r.name AS Recipe, r.calories AS Calories,
               r.preparation_description AS Preparation
        LIMIT {limit}
        """
        
        return st.session_state.connection.execute_query_to_df(query, allergen_params)
    
    def find_recipe_ingredients(self, recipe_name: str) -> List[str]:
        """Get ingredients for a specific recipe."""
        if not st.session_state.connected:
            return []
        
        query = """
        MATCH (r:Recipe {name: $recipe_name})-[:CONTAINS]->(i:Ingredient)
        RETURN i.name AS Ingredient
        """
        
        df = st.session_state.connection.execute_query_to_df(query, {"recipe_name": recipe_name})
        return df["Ingredient"].tolist() if not df.empty else []
    
    def find_personalized_recipes(
        self, 
        diet_preferences: List[str], 
        allergies: List[str],
        min_calories: Optional[int] = None,
        max_calories: Optional[int] = None,
        limit: int = 10
    ) -> pd.DataFrame:
        """
        Find recipes based on user's dietary preferences and allergies.
        
        This is the main recommendation function that combines dietary preferences
        and allergy restrictions to find suitable recipes.
        """
        if not st.session_state.connected:
            return pd.DataFrame()
        
        # Build query parameters
        params = {}
        
        # Diet preference conditions - updated to use both INCLUDES and EXCLUDES
        diet_condition = ""
        if diet_preferences:
            diet_params = {f"diet{i}": pref for i, pref in enumerate(diet_preferences)}
            params.update(diet_params)
            
            # Recipes should be included by all selected diet preferences
            include_conditions = []
            for i, _ in enumerate(diet_preferences):
                include_conditions.append(f"EXISTS {{ MATCH (r)<-[:INCLUDES]-(dp:DietPreference {{name: $diet{i}}}) }}")
            
            # And should not be excluded by any selected diet preferences
            exclude_conditions = []
            for i, _ in enumerate(diet_preferences):
                exclude_conditions.append(f"NOT EXISTS {{ MATCH (r)<-[:EXCLUDES]-(dp:DietPreference {{name: $diet{i}}}) }}")
            
            diet_condition = f"""
            AND {" AND ".join(include_conditions)}
            AND {" AND ".join(exclude_conditions)}
            """
        
        # Allergy conditions - updated for matching allergy relationships
        allergy_condition = ""
        if allergies:
            allergy_params = {f"allergen{i}": allergen for i, allergen in enumerate(allergies)}
            params.update(allergy_params)
            
            # Either using the direct relationship to avoid allergens
            direct_allergy_conditions = [
                f"NOT EXISTS {{ MATCH (r)-[:MAY_CONTAIN_ALLERGEN]->(:Allergy {{name: $allergen{i}}}) }}"
                for i in range(len(allergies))
            ]
            
            # Or via ingredient-allergy relationships
            ingredient_allergy_conditions = [
                f"NOT EXISTS {{ MATCH (r)-[:CONTAINS]->(:Ingredient)<-[:CAUSES]-(:Allergy {{name: $allergen{i}}}) }}"
                for i in range(len(allergies))
            ]
            
            # Combine both approaches
            all_allergy_conditions = []
            for i in range(len(allergies)):
                all_allergy_conditions.append(f"({direct_allergy_conditions[i]} AND {ingredient_allergy_conditions[i]})")
            
            allergy_condition = f"""
            AND {" AND ".join(all_allergy_conditions)}
            """
        
        # Calorie conditions
        calorie_condition = ""
        if min_calories is not None:
            params["min_calories"] = min_calories
            calorie_condition += " AND r.calories >= $min_calories"
        if max_calories is not None:
            params["max_calories"] = max_calories
            calorie_condition += " AND r.calories <= $max_calories"
        
        # Construct the main query
        query = f"""
        MATCH (r:Recipe)
        WHERE true
        {diet_condition}
        {allergy_condition}
        {calorie_condition}
        RETURN r.name AS Recipe, r.calories AS Calories, 
               r.preparation_description AS Preparation
        ORDER BY r.calories ASC
        LIMIT {limit}
        """
        
        return st.session_state.connection.execute_query_to_df(query, params)
    
    def save_recipe_preference(self, person_id: str, recipe_name: str, rating: int = 5) -> bool:
        """
        Save user preference for a recipe by creating a LIKES relationship.
        
        Args:
            person_id: User ID (create a placeholder person if needed)
            recipe_name: Name of the recipe to save
            rating: Rating from 1-5
            
        Returns:
            bool: Success status
        """
        if not st.session_state.connected:
            return False
            
        try:
            # First ensure the person exists
            create_person_query = """
            MERGE (p:Person {id: $person_id})
            RETURN p
            """
            
            # Then create the LIKES relationship with rating
            # Use CREATE instead of MERGE to avoid issues if the relationship doesn't exist yet
            save_pref_query = """
            MATCH (p:Person {id: $person_id})
            MATCH (r:Recipe {name: $recipe_name})
            CREATE (p)-[l:LIKES {rating: $rating, timestamp: timestamp()}]->(r)
            RETURN l
            """
            
            with st.session_state.connection.get_driver().session() as session:
                # Create or find the person
                session.run(create_person_query, {"person_id": person_id})
                
                # Create the relationship
                result = session.run(save_pref_query, {
                    "person_id": person_id,
                    "recipe_name": recipe_name,
                    "rating": rating
                })
                
                # Add to session state favorites regardless of database result
                if recipe_name not in st.session_state.favorite_recipes:
                    st.session_state.favorite_recipes.append(recipe_name)
                
                # Return true even if the relationship couldn't be created
                # This allows the UI to still function with local favorites
                return True
                
        except Exception as e:
            st.error(f"Error saving recipe preference: {e}")
            # Still add to session state favorites for local state persistence
            if recipe_name not in st.session_state.favorite_recipes:
                st.session_state.favorite_recipes.append(recipe_name)
            return True  # Return true to keep UI functional
    
    def get_saved_recipes(self, person_id: str) -> pd.DataFrame:
        """
        Get all recipes saved by a user.
        
        Args:
            person_id: User ID
            
        Returns:
            DataFrame with saved recipes and ratings
        """
        if not st.session_state.connected:
            return pd.DataFrame()
            
        try:
            # Try to query database first
            query = """
            MATCH (p:Person {id: $person_id})-[l:LIKES]->(r:Recipe)
            RETURN r.name AS Recipe, r.calories AS Calories,
                   l.rating AS Rating, l.timestamp AS SavedOn
            ORDER BY l.timestamp DESC
            """
            
            df = st.session_state.connection.execute_query_to_df(query, {"person_id": person_id})
            
            # If we got results from the database, return them
            if not df.empty:
                return df
                
            # If database query returned no results, fall back to session state
            if 'favorite_recipes' in st.session_state and st.session_state.favorite_recipes:
                # Create a dataframe from the list of favorites stored in session state
                recipe_data = []
                for recipe in st.session_state.favorite_recipes:
                    # Try to get recipe details from database
                    details_query = """
                    MATCH (r:Recipe {name: $recipe_name})
                    RETURN r.name AS Recipe, r.calories AS Calories
                    """
                    
                    details_df = st.session_state.connection.execute_query_to_df(
                        details_query, 
                        {"recipe_name": recipe}
                    )
                    
                    if not details_df.empty:
                        row_dict = details_df.iloc[0].to_dict()
                        # Add default rating and timestamp
                        row_dict['Rating'] = 5  # Default rating
                        row_dict['SavedOn'] = pd.Timestamp.now()  # Current time
                        recipe_data.append(row_dict)
                    else:
                        # Even if we can't get details, still include the recipe name
                        recipe_data.append({
                            'Recipe': recipe,
                            'Calories': None,
                            'Rating': 5,
                            'SavedOn': pd.Timestamp.now()
                        })
                
                if recipe_data:
                    return pd.DataFrame(recipe_data)
            
            return pd.DataFrame()  # Return empty DataFrame if no saved recipes
            
        except Exception as e:
            st.error(f"Error retrieving saved recipes: {e}")
            
            # Fall back to session state on error
            if 'favorite_recipes' in st.session_state and st.session_state.favorite_recipes:
                # Create a simple dataframe with just recipe names from session state
                recipe_data = [{
                    'Recipe': recipe,
                    'Rating': 5,
                    'SavedOn': pd.Timestamp.now()
                } for recipe in st.session_state.favorite_recipes]
                
                return pd.DataFrame(recipe_data)
                
            return pd.DataFrame()
    
    def run(self):
        """Run the Streamlit dashboard."""
        st.title("🍲 Personalized Recipe Recommendations")
        
        st.sidebar.title("Database Connection")
        if st.sidebar.button("Connect to Database"):
            with st.sidebar:
                with st.spinner("Connecting to database..."):
                    if self.connect_to_database():
                        st.success("Connected to database!")
                    else:
                        st.error("Failed to connect to database.")
        
        # Main content
        if not self.connected:
            st.info("Please connect to the database using the sidebar to start.")
            return
        
        # Create tabs for recommendations and saved recipes
        tab1, tab2 = st.tabs(["Find Recommendations", "My Saved Recipes"])
        
        with tab1:
            # User input section
            st.header("Your Dietary Preferences")
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("Diet Preferences")
                selected_diets = st.multiselect(
                    "Select your diet preferences:",
                    options=self.diet_preferences
                )
            
            with col2:
                st.subheader("Allergies")
                selected_allergies = st.multiselect(
                    "Select allergies to avoid:",
                    options=self.allergies
                )
            
            # Additional filters
            st.subheader("Additional Filters")
            col1, col2 = st.columns(2)
            
            with col1:
                min_calories = st.number_input("Minimum Calories", min_value=0, value=0)
            
            with col2:
                max_calories = st.number_input("Maximum Calories", min_value=0, value=2000)
            
            # Number of recommendations
            num_recommendations = st.slider(
                "Number of recommendations",
                min_value=1,
                max_value=20,
                value=5
            )
            
            # Find recommendations button
            if st.button("Find Recipe Recommendations"):
                with st.spinner("Finding recipes for you..."):
                    # Apply min_calories only if greater than 0
                    min_cal = min_calories if min_calories > 0 else None
                    # Apply max_calories only if greater than 0
                    max_cal = max_calories if max_calories > 0 else None
                    
                    # Get recipe recommendations
                    recipes_df = self.find_personalized_recipes(
                        diet_preferences=selected_diets,
                        allergies=selected_allergies,
                        min_calories=min_cal,
                        max_calories=max_cal,
                        limit=num_recommendations
                    )
                    
                    if recipes_df.empty:
                        st.warning("No recipes found matching your criteria. Try adjusting your preferences.")
                    else:
                        st.success(f"Found {len(recipes_df)} recipes for you!")
                        
                        # Store recipes in session state for selecting
                        st.session_state.current_recommendations = recipes_df
                        
                        # Display recipes with selection buttons
                        for i, (_, row) in enumerate(recipes_df.iterrows()):
                            with st.expander(f"{i+1}. {row['Recipe']} ({row['Calories']} calories)"):
                                # Get ingredients
                                ingredients = self.find_recipe_ingredients(row['Recipe'])
                                
                                st.subheader("Ingredients")
                                st.write(", ".join(ingredients))
                                
                                st.subheader("Preparation")
                                if pd.notna(row['Preparation']):
                                    st.write(row['Preparation'])
                                else:
                                    st.write("No preparation details available.")
                                
                                # Add save button for each recipe
                                col1, col2, col3 = st.columns([1, 1, 3])
                                
                                rating = col1.selectbox(
                                    "Rating", 
                                    options=[1, 2, 3, 4, 5],
                                    index=4,  # Default to 5
                                    key=f"rating_{i}"
                                )
                                
                                if col2.button("Save Recipe", key=f"save_{i}"):
                                    if self.save_recipe_preference(
                                        st.session_state.user_id, 
                                        row['Recipe'],
                                        rating
                                    ):
                                        st.success(f"Saved {row['Recipe']} to your favorites!")
                                        
                                        # Add to session state favorites if not already there
                                        if row['Recipe'] not in st.session_state.favorite_recipes:
                                            st.session_state.favorite_recipes.append(row['Recipe'])
                                    else:
                                        st.error("Failed to save recipe. Please try again.")
        
        with tab2:
            st.header("Your Saved Recipes")
            
            # Refresh button to load the latest saved recipes
            if st.button("Refresh Saved Recipes"):
                pass  # The refresh happens by default when we access this tab
            
            # Get saved recipes from database
            saved_recipes_df = self.get_saved_recipes(st.session_state.user_id)
            
            if saved_recipes_df.empty:
                st.info("You haven't saved any recipes yet. Find and save some recipes from the recommendations tab!")
            else:
                st.success(f"You have {len(saved_recipes_df)} saved recipes!")
                
                # Display saved recipes
                for i, (_, row) in enumerate(saved_recipes_df.iterrows()):
                    with st.expander(f"{i+1}. {row['Recipe']} - Rating: {row['Rating']} ⭐"):
                        # Get ingredients
                        ingredients = self.find_recipe_ingredients(row['Recipe'])
                        
                        st.subheader("Ingredients")
                        st.write(", ".join(ingredients))
                        
                        # If we have calorie info
                        if 'Calories' in row and pd.notna(row['Calories']):
                            st.write(f"**Calories:** {row['Calories']}")
                            
                        # Get recipe details - we might need an additional query
                        recipe_details_query = """
                        MATCH (r:Recipe {name: $recipe_name})
                        RETURN r.preparation_description AS Preparation
                        """
                        
                        details_df = st.session_state.connection.execute_query_to_df(
                            recipe_details_query, 
                            {"recipe_name": row['Recipe']}
                        )
                        
                        if not details_df.empty and pd.notna(details_df.iloc[0]['Preparation']):
                            st.subheader("Preparation")
                            st.write(details_df.iloc[0]['Preparation'])
                        else:
                            st.write("No preparation details available.")


# Main application
if __name__ == "__main__":
    dashboard = RecipeRecommendationDashboard()
    dashboard.run()