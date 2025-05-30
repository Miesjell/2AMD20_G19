"""
Advanced Recipe Recommendation Dashboard

A sophisticated Streamlit application providing personalized recipe recommendations,
nutritional insights, and food analytics. Features include smart recipe search,
dietary compatibility checking, ingredient analysis, and interactive visualizations.
Connected to a Neo4j graph database containing recipes, ingredients, allergies, and diet preferences.
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional
import time

# Import Neo4j connection modules from the existing project
from src.graph_db.neo4j.connection import Neo4jConnection
from src.graph_db.queries.manager import QueryManager

# Set page config
st.set_page_config(
    page_title="🍲 Smart Recipe Recommendations",
    page_icon="🍲",
    layout="wide",
    initial_sidebar_state="expanded",
    menu_items={
        'Get Help': 'https://docs.streamlit.io/',
        'Report a bug': None,
        'About': "# Smart Recipe Dashboard\nPowered by Neo4j Graph Database"
    }
)

# Custom CSS for enhanced styling
def apply_custom_css():
    """Apply custom CSS for a modern, clean look."""
    st.markdown("""
    <style>
    .main {
        background: linear-gradient(135deg, #f8f9fa 0%, #e9ecef 100%);
        padding: 2rem;
    }
    
    .stApp {
        background: #f8f9fa;
    }
    
    .metric-card {
        background: white;
        padding: 1rem;
        border-radius: 10px;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
        margin: 0.5rem 0;
        border: 1px solid #e9ecef;
    }
    
    .recipe-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        padding: 1.5rem;
        border-radius: 15px;
        margin: 1rem 0;
        box-shadow: 0 8px 16px rgba(0, 0, 0, 0.2);
    }
    
    .stTabs [data-baseweb="tab-list"] {
        gap: 8px;
    }
    
    .stTabs [data-baseweb="tab"] {
        height: 50px;
        padding-left: 20px;
        padding-right: 20px;
        background-color: #f0f2f6;
        border-radius: 10px;
        border: none;
    }
    
    .stTabs [aria-selected="true"] {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
    }
    
    .success-box {
        background: linear-gradient(135deg, #11998e 0%, #38ef7d 100%);
        color: white;
        padding: 1rem;
        border-radius: 10px;
        margin: 1rem 0;
    }
    
    .warning-box {
        background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%);
        color: white;
        padding: 1rem;
        border-radius: 10px;
        margin: 1rem 0;
    }
    
    .info-box {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        padding: 1rem;
        border-radius: 10px;
        margin: 1rem 0;
    }
    
    .sidebar .stSelectbox, .sidebar .stMultiSelect {
        background-color: white;
        border-radius: 5px;
    }
    
    .stSelectbox > div > div {
        background-color: white;
        border-radius: 5px;
    }
    
    .stMultiSelect > div > div {
        background-color: white;
        border-radius: 5px;
    }
    
    .stButton > button {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        border: none;
        border-radius: 10px;
        padding: 0.5rem 1rem;
        font-weight: bold;
        transition: all 0.3s ease;
    }
    
    .stButton > button:hover {
        transform: translateY(-2px);
        box-shadow: 0 8px 16px rgba(0, 0, 0, 0.2);
    }
    
    .stExpander {
        border-radius: 10px;
        box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
    }
    
    .recipe-ingredient {
        background: rgba(255, 255, 255, 0.1);
        padding: 0.5rem;
        border-radius: 5px;
        margin: 0.2rem;
        display: inline-block;
    }
    
    .dashboard-card {
        background: white;
        border: 1px solid #e9ecef;
        border-radius: 15px;
        padding: 1.5rem;
        margin: 1rem 0;
        transition: all 0.3s ease;
        box-shadow: 0 2px 10px rgba(0, 0, 0, 0.1);
    }
    
    .dashboard-card:hover {
        transform: translateY(-2px);
        box-shadow: 0 8px 25px rgba(0, 0, 0, 0.15);
    }
    
    .nutrition-badge {
        background: linear-gradient(135deg, #11998e 0%, #38ef7d 100%);
        color: white;
        padding: 0.3rem 0.8rem;
        border-radius: 20px;
        font-size: 0.8rem;
        margin: 0.2rem;
        display: inline-block;
        font-weight: bold;
    }
    
    .complexity-simple {
        background: linear-gradient(135deg, #11998e 0%, #38ef7d 100%);
    }
    
    .complexity-moderate {
        background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%);
    }
    
    .complexity-complex {
        background: linear-gradient(135deg, #4facfe 0%, #00f2fe 100%);
    }
    
    .meal-plan-card {
        background: linear-gradient(135deg, rgba(255,255,255,0.15) 0%, rgba(255,255,255,0.05) 100%);
        border-radius: 12px;
        padding: 1rem;
        margin: 0.5rem;
        border: 1px solid rgba(255,255,255,0.1);
        backdrop-filter: blur(5px);
    }
    
    .feature-highlight {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        padding: 2rem;
        border-radius: 15px;
        margin: 1rem 0;
        text-align: center;
        box-shadow: 0 10px 30px rgba(102, 126, 234, 0.3);
    }
    
    .animated-metric {
        animation: pulse 2s infinite;
    }
    
    @keyframes pulse {
        0% { transform: scale(1); }
        50% { transform: scale(1.05); }
        100% { transform: scale(1); }
    }
    
    .gradient-text {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        background-clip: text;
        font-weight: bold;
        color: #667eea; /* Fallback for unsupported browsers */
    }
    
    .floating-card {
        position: relative;
        overflow: hidden;
    }
    
    .floating-card::before {
        content: '';
        position: absolute;
        top: 0;
        left: -100%;
        width: 100%;
        height: 100%;
        background: linear-gradient(90deg, transparent, rgba(255,255,255,0.1), transparent);
        transition: left 0.5s;
    }
    
    .floating-card:hover::before {
        left: 100%;
    }
    </style>
    """, unsafe_allow_html=True)

# Create a class for our recipe recommendation dashboard
class SmartRecipeRecommendationDashboard:
    """
    An advanced Streamlit dashboard providing intelligent recipe recommendations,
    nutritional insights, and comprehensive food analytics with modern UI.
    """
    
    def __init__(self):
        """Initialize the dashboard with database connection and enhanced session state."""
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
        if 'meal_types' not in st.session_state:
            st.session_state.meal_types = []
        if 'favorite_recipes' not in st.session_state:
            st.session_state.favorite_recipes = []
        if 'user_id' not in st.session_state:
            # Generate a simple user ID for the session
            st.session_state.user_id = f"user_{np.random.randint(10000)}"
        if 'search_history' not in st.session_state:
            st.session_state.search_history = []
        if 'recipe_analytics' not in st.session_state:
            st.session_state.recipe_analytics = {}
        
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
    
    @property
    def meal_types(self):
        """Get meal types from session state."""
        return st.session_state.meal_types
    
    def get_recipe_analytics(self) -> Dict[str, Any]:
        """Get comprehensive analytics about recipes in the database."""
        if not st.session_state.connected:
            return {}
        
        try:
            # Get total counts
            total_recipes_query = "MATCH (r:Recipe) RETURN count(r) AS total_recipes"
            total_ingredients_query = "MATCH (i:Ingredient) RETURN count(i) AS total_ingredients"
            total_allergies_query = "MATCH (a:Allergy) RETURN count(a) AS total_allergies"
            
            total_recipes = st.session_state.connection.execute_query_to_df(total_recipes_query, {})
            total_ingredients = st.session_state.connection.execute_query_to_df(total_ingredients_query, {})
            total_allergies = st.session_state.connection.execute_query_to_df(total_allergies_query, {})
            
            # Get calorie statistics
            calorie_stats_query = """
            MATCH (r:Recipe) 
            WHERE r.calories IS NOT NULL 
            RETURN 
                avg(r.calories) AS avg_calories,
                min(r.calories) AS min_calories,
                max(r.calories) AS max_calories,
                count(r) AS recipes_with_calories
            """
            calorie_stats = st.session_state.connection.execute_query_to_df(calorie_stats_query, {})
            
            # Get popular ingredients
            popular_ingredients = st.session_state.query_manager.find_popular_ingredients(10)
            
            # Get meal type distribution
            meal_type_query = """
            MATCH (r:Recipe)-[:IS_TYPE]->(m:MealType)
            RETURN m.name AS meal_type, count(r) AS recipe_count
            ORDER BY recipe_count DESC
            """
            meal_distribution = st.session_state.connection.execute_query_to_df(meal_type_query, {})
            
            return {
                'total_recipes': total_recipes.iloc[0]['total_recipes'] if not total_recipes.empty else 0,
                'total_ingredients': total_ingredients.iloc[0]['total_ingredients'] if not total_ingredients.empty else 0,
                'total_allergies': total_allergies.iloc[0]['total_allergies'] if not total_allergies.empty else 0,
                'calorie_stats': calorie_stats.to_dict('records')[0] if not calorie_stats.empty else {},
                'popular_ingredients': popular_ingredients,
                'meal_distribution': meal_distribution
            }
        except Exception as e:
            st.error(f"Error getting analytics: {e}")
            return {}
    
    def search_recipes_by_name(self, search_term: str, limit: int = 10) -> pd.DataFrame:
        """Search recipes by name with fuzzy matching."""
        if not st.session_state.connected or not search_term:
            return pd.DataFrame()
        
        query = f"""
        MATCH (r:Recipe)
        WHERE toLower(r.name) CONTAINS toLower($search_term)
        RETURN r.name AS Recipe, r.calories AS Calories, 
               r.preparation_description AS Preparation
        ORDER BY r.name
        LIMIT {limit}
        """
        
        # Add to search history
        if search_term not in st.session_state.search_history:
            st.session_state.search_history.append(search_term)
            # Keep only last 10 searches
            st.session_state.search_history = st.session_state.search_history[-10:]
        
        return st.session_state.connection.execute_query_to_df(query, {"search_term": search_term})
    
    def get_ingredient_insights(self, ingredient_name: str) -> Dict[str, Any]:
        """Get detailed insights about a specific ingredient."""
        if not st.session_state.connected:
            return {}
        
        try:
            # Recipes containing this ingredient
            recipes_query = """
            MATCH (i:Ingredient {name: $ingredient_name})<-[:CONTAINS]-(r:Recipe)
            RETURN count(r) AS recipe_count
            """
            
            # Allergies related to this ingredient
            allergies_query = """
            MATCH (i:Ingredient {name: $ingredient_name})-[:CAUSES]->(a:Allergy)
            RETURN collect(a.name) AS related_allergies
            """
            
            recipe_count = st.session_state.connection.execute_query_to_df(
                recipes_query, {"ingredient_name": ingredient_name}
            )
            allergies = st.session_state.connection.execute_query_to_df(
                allergies_query, {"ingredient_name": ingredient_name}
            )
            
            return {
                'recipe_count': recipe_count.iloc[0]['recipe_count'] if not recipe_count.empty else 0,
                'related_allergies': allergies.iloc[0]['related_allergies'] if not allergies.empty else []
            }
        except Exception as e:
            st.error(f"Error getting ingredient insights: {e}")
            return {}
    
    def create_calorie_distribution_chart(self) -> go.Figure:
        """Create a histogram of calorie distribution."""
        if not st.session_state.connected:
            return go.Figure()
        
        try:
            query = """
            MATCH (r:Recipe)
            WHERE r.calories IS NOT NULL AND r.calories > 0
            RETURN r.calories AS calories
            """
            
            df = st.session_state.connection.execute_query_to_df(query, {})
            
            if df.empty:
                return go.Figure()
            
            fig = px.histogram(
                df, 
                x='calories', 
                bins=30,
                title='Recipe Calorie Distribution',
                labels={'calories': 'Calories per Recipe', 'count': 'Number of Recipes'},
                color_discrete_sequence=['#667eea']
            )
            
            fig.update_layout(
                plot_bgcolor='white',
                paper_bgcolor='white',
                font=dict(color='black'),
                title_font_color='black'
            )
            
            return fig
        except Exception as e:
            st.error(f"Error creating chart: {e}")
            return go.Figure()
    
    def create_meal_type_chart(self) -> go.Figure:
        """Create a pie chart of meal type distribution."""
        analytics = self.get_recipe_analytics()
        meal_distribution = analytics.get('meal_distribution', pd.DataFrame())
        
        if meal_distribution.empty:
            return go.Figure()
        
        fig = px.pie(
            meal_distribution, 
            values='recipe_count', 
            names='meal_type',
            title='Recipe Distribution by Meal Type',
            color_discrete_sequence=px.colors.qualitative.Set3
        )
        
        fig.update_layout(
            plot_bgcolor='white',
            paper_bgcolor='white',
            font=dict(color='black'),
            title_font_color='black'
        )
        
        return fig
    
    def create_ingredient_popularity_chart(self) -> go.Figure:
        """Create a bar chart of most popular ingredients."""
        analytics = self.get_recipe_analytics()
        popular_ingredients = analytics.get('popular_ingredients', pd.DataFrame())
        
        if popular_ingredients.empty:
            return go.Figure()
        
        fig = px.bar(
            popular_ingredients.head(10), 
            x='RecipeCount', 
            y='Ingredient',
            orientation='h',
            title='Top 10 Most Popular Ingredients',
            labels={'RecipeCount': 'Number of Recipes', 'Ingredient': 'Ingredient'},
            color='RecipeCount',
            color_continuous_scale='Viridis'
        )
        
        fig.update_layout(
            plot_bgcolor='white',
            paper_bgcolor='white',
            font=dict(color='black'),
            title_font_color='black',
            yaxis={'categoryorder': 'total ascending'}
        )
        
        return fig
        
    def display_metrics_dashboard(self):
        """Display key metrics in an attractive dashboard format with enhanced styling."""
        analytics = self.get_recipe_analytics()
        
        # Main metrics row
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            total_recipes = analytics.get('total_recipes', 0)
            st.markdown(f"""
            <div class="dashboard-card floating-card animated-metric">
                <h3 class="gradient-text">Total Recipes</h3>
                <h1 style="color: #333; margin: 0; font-size: 2.5rem;">{total_recipes:,}</h1>
                <p style="color: #666; margin: 0;">Available in database</p>
            </div>
            """, unsafe_allow_html=True)
        
        with col2:
            total_ingredients = analytics.get('total_ingredients', 0)
            st.markdown(f"""
            <div class="dashboard-card floating-card animated-metric">
                <h3 class="gradient-text">Unique Ingredients</h3>
                <h1 style="color: #333; margin: 0; font-size: 2.5rem;">{total_ingredients:,}</h1>
                <p style="color: #666; margin: 0;">Ingredient variety</p>
            </div>
            """, unsafe_allow_html=True)
        
        with col3:
            avg_calories = analytics.get('calorie_stats', {}).get('avg_calories', 0)
            calorie_color = "#27ae60" if avg_calories < 400 else "#f39c12" if avg_calories < 600 else "#e74c3c"
            st.markdown(f"""
            <div class="dashboard-card floating-card animated-metric">
                <h3 class="gradient-text">Avg Calories</h3>
                <h1 style="color: {calorie_color}; margin: 0; font-size: 2.5rem;">{avg_calories:.0f}</h1>
                <p style="color: #666; margin: 0;">Per recipe</p>
            </div>
            """, unsafe_allow_html=True)
        
        with col4:
            fav_count = len(st.session_state.favorite_recipes)
            st.markdown(f"""
            <div class="dashboard-card floating-card animated-metric">
                <h3 class="gradient-text">Your Favorites</h3>
                <h1 style="color: #e74c3c; margin: 0; font-size: 2.5rem;">{fav_count}</h1>
                <p style="color: #666; margin: 0;">Saved recipes</p>
            </div>
            """, unsafe_allow_html=True)
        
        # Additional insights row
        if analytics.get('calorie_stats'):
            insight_col1, insight_col2 = st.columns(2)
            
            with insight_col1:
                min_cal = analytics['calorie_stats'].get('min_calories', 0)
                max_cal = analytics['calorie_stats'].get('max_calories', 0)
                st.markdown(f"""
                <div class="dashboard-card">
                    <h4 style="color: #667eea;">📊 Calorie Range</h4>
                    <p style="margin: 0.5rem 0;">
                        <strong>Lowest:</strong> {min_cal:.0f} cal<br>
                        <strong>Highest:</strong> {max_cal:.0f} cal<br>
                        <strong>Range:</strong> {max_cal - min_cal:.0f} cal
                    </p>
                </div>
                """, unsafe_allow_html=True)
            
            with insight_col2:
                recipes_with_cal = analytics['calorie_stats'].get('recipes_with_calories', 0)
                coverage = (recipes_with_cal / analytics.get('total_recipes', 1)) * 100 if analytics.get('total_recipes', 0) > 0 else 0
                st.markdown(f"""
                <div class="dashboard-card">
                    <h4 style="color: #667eea;">🔍 Data Coverage</h4>
                    <p style="margin: 0.5rem 0;">
                        <strong>With Calories:</strong> {recipes_with_cal:,} recipes<br>
                        <strong>Coverage:</strong> {coverage:.1f}%<br>
                        <strong>Quality:</strong> {'Excellent' if coverage > 80 else 'Good' if coverage > 60 else 'Fair'}
                    </p>
                </div>
                """, unsafe_allow_html=True)
    
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
                
                # Load diet preferences, allergies, and meal types
                self._load_diet_preferences()
                self._load_allergies()
                self._load_meal_types()
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
    
    def _load_meal_types(self):
        """Load available meal types from the database."""
        if not st.session_state.connected:
            return
        
        try:
            # Direct query to get meal types
            query = """
            MATCH (m:MealType)
            RETURN DISTINCT m.name AS MealType
            ORDER BY m.name
            """
            
            df = st.session_state.connection.execute_query_to_df(query, {})
            
            if df.empty:
                # Fallback to standard meal types
                st.session_state.meal_types = [
                    "Breakfast", "Lunch", "Dinner", "Drink", "Other"
                ]
                st.sidebar.warning("No meal types found in database, using defaults")
            else:
                st.session_state.meal_types = df["MealType"].tolist()
                
        except Exception as e:
            st.error(f"Error loading meal types: {e}")
            # Fallback to standard meal types
            st.session_state.meal_types = [
                "Breakfast", "Lunch", "Dinner", "Drink", "Other"
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
    
    def find_recipes_by_meal_type(self, meal_type: str, limit: int = 10) -> pd.DataFrame:
        """Find recipes based on meal type."""
        if not st.session_state.connected:
            return pd.DataFrame()
        
        query = f"""
        MATCH (r:Recipe)-[:IS_TYPE]->(:MealType {{name: $meal_type}})
        RETURN r.name AS Recipe, r.calories AS Calories, 
               r.preparation_description AS Preparation
        LIMIT {limit}
        """
        
        return st.session_state.connection.execute_query_to_df(query, {"meal_type": meal_type})
    
    def find_personalized_recipes(
        self, 
        diet_preferences: List[str], 
        allergies: List[str],
        meal_type: Optional[str] = None,
        min_calories: Optional[int] = None,
        max_calories: Optional[int] = None,
        limit: int = 10
    ) -> pd.DataFrame:
        """
        Find recipes based on user's dietary preferences and allergies.
        
        This is the main recommendation function that combines dietary preferences
        and allergy restrictions to find suitable recipes.
        
        Args:
            diet_preferences: List of diet preference names
            allergies: List of allergy names to avoid
            meal_type: Optional meal type filter (breakfast, lunch, dinner, etc.)
            min_calories: Optional minimum calorie count
            max_calories: Optional maximum calorie count
            limit: Maximum number of recipes to return
            
        Returns:
            DataFrame with matched recipes
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
        
        # Meal type condition
        meal_type_condition = ""
        if meal_type and meal_type != "All Types":
            params["meal_type"] = meal_type
            meal_type_condition = """
            AND EXISTS {
                MATCH (r)-[:IS_TYPE]->(:MealType {name: $meal_type})
            }
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
        {meal_type_condition}
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
        """Run the enhanced Streamlit dashboard with modern UI and advanced features."""
        # Apply custom CSS
        apply_custom_css()
        
        # Header with gradient background
        st.markdown("""
        <div style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); 
                    padding: 2rem; border-radius: 15px; margin-bottom: 2rem; color: white;">
            <h1 style="text-align: center; margin: 0; font-size: 3rem;">
                🍲 Smart Recipe Recommendations
            </h1>
            <p style="text-align: center; margin: 0; font-size: 1.2rem; opacity: 0.9;">
                AI-Powered Personalized Nutrition & Recipe Discovery
            </p>
        </div>
        """, unsafe_allow_html=True)
        
        # Sidebar for database connection and quick actions
        with st.sidebar:
            st.markdown("### 🔗 Database Connection")
            
            if not self.connected:
                if st.button("🚀 Connect to Database", use_container_width=True):
                    with st.spinner("Establishing connection..."):
                        if self.connect_to_database():
                            st.success("✅ Connected successfully!")
                            time.sleep(1)
                            st.rerun()
                        else:
                            st.error("❌ Connection failed.")
            else:
                st.success("✅ Connected to Neo4j Database")
                
                # Quick stats in sidebar
                st.markdown("### 📊 Quick Stats")
                analytics = self.get_recipe_analytics()
                
                st.metric("Recipes", analytics.get('total_recipes', 0))
                st.metric("Ingredients", analytics.get('total_ingredients', 0))
                st.metric("Your Favorites", len(st.session_state.favorite_recipes))
                
                # Recent searches
                if st.session_state.search_history:
                    st.markdown("### 🔍 Recent Searches")
                    for search in st.session_state.search_history[-3:]:
                        st.markdown(f"• {search}")
        
        # Main content
        if not self.connected:
            st.markdown("""
            <div class="feature-highlight">
                <h2>🎯 Welcome to Smart Recipe Recommendations!</h2>
                <p style="font-size: 1.1rem; margin: 1rem 0;">
                    Your AI-powered culinary companion for personalized nutrition and recipe discovery
                </p>
                <p>Connect to unlock these amazing features:</p>
            </div>
            """, unsafe_allow_html=True)
            
            # Feature showcase
            feat_col1, feat_col2, feat_col3 = st.columns(3)
            
            with feat_col1:
                st.markdown("""
                <div class="dashboard-card">
                    <h4 style="color: #667eea;">🍽️ Smart Recommendations</h4>
                    <ul>
                        <li>AI-powered recipe matching</li>
                        <li>Dietary preference filtering</li>
                        <li>Allergy-safe suggestions</li>
                    </ul>
                </div>
                """, unsafe_allow_html=True)
            
            with feat_col2:
                st.markdown("""
                <div class="dashboard-card">
                    <h4 style="color: #667eea;">📊 Analytics & Insights</h4>
                    <ul>
                        <li>Nutritional analysis</li>
                        <li>Recipe complexity scoring</li>
                        <li>Ingredient popularity trends</li>
                    </ul>
                </div>
                """, unsafe_allow_html=True)
            
            with feat_col3:
                st.markdown("""
                <div class="dashboard-card">
                    <h4 style="color: #667eea;">📅 Meal Planning</h4>
                    <ul>
                        <li>Automated meal plans</li>
                        <li>Calorie target tracking</li>
                        <li>Recipe comparison tools</li>
                    </ul>
                </div>
                """, unsafe_allow_html=True)
            
            return
        
        # Display metrics dashboard
        self.display_metrics_dashboard()
        
        # Create enhanced tabs
        tab1, tab2, tab3, tab4, tab5, tab6, tab7 = st.tabs([
            "🎯 Smart Recommendations", 
            "🔍 Recipe Search", 
            "❤️ My Favorites", 
            "📊 Analytics",
            "🧪 Ingredient Insights",
            "📅 Meal Planning",
            "⚖️ Recipe Comparison"
        ])
        
        with tab1:
            self._render_recommendations_tab()
        
        with tab2:
            self._render_search_tab()
        
        with tab3:
            self._render_favorites_tab()
        
        with tab4:
            self._render_analytics_tab()
        
        with tab5:
            self._render_ingredient_insights_tab()
        
        with tab6:
            self._render_meal_planning_tab()
        
        with tab7:
            self._render_recipe_comparison_tab()
    
    def _render_recommendations_tab(self):
        """Render the smart recommendations tab with enhanced UI."""
        st.header("🎯 Personalized Recipe Recommendations")
        
        # Enhanced user input section with better layout
        with st.container():
            st.markdown("### 👤 Your Preferences")
            
            col1, col2 = st.columns(2)
            
            with col1:
                st.markdown("#### 🥗 Dietary Preferences")
                selected_diets = st.multiselect(
                    "What dietary preferences do you follow?",
                    options=self.diet_preferences,
                    help="Select all that apply to get the most relevant recommendations"
                )
                
                st.markdown("#### 🍽️ Meal Type")
                selected_meal_type = st.selectbox(
                    "What type of meal are you looking for?",
                    options=["All Types"] + self.meal_types,
                    help="Filter recipes by meal category"
                )
            
            with col2:
                st.markdown("#### ⚠️ Allergies & Restrictions")
                selected_allergies = st.multiselect(
                    "What allergies or ingredients should we avoid?",
                    options=self.allergies,
                    help="We'll make sure to exclude these from your recommendations"
                )
                
                st.markdown("#### 🔥 Calorie Range")
                col_a, col_b = st.columns(2)
                with col_a:
                    min_calories = st.number_input("Min Calories", min_value=0, value=0, help="Minimum calories per serving")
                with col_b:
                    max_calories = st.number_input("Max Calories", min_value=0, value=2000, help="Maximum calories per serving")
        
        # Advanced filters in expander
        with st.expander("🔧 Advanced Filters", expanded=False):
            col1, col2 = st.columns(2)
            with col1:
                num_recommendations = st.slider(
                    "Number of recommendations",
                    min_value=1,
                    max_value=20,
                    value=8,
                    help="How many recipes would you like to see?"
                )
            with col2:
                sort_by = st.selectbox(
                    "Sort recommendations by:",
                    ["Calories (Low to High)", "Calories (High to Low)", "Recipe Name"],
                    help="Choose how to order your results"
                )
        
        # Enhanced find recommendations button
        if st.button("🔍 Find My Perfect Recipes", use_container_width=True, type="primary"):
            with st.spinner("🤖 AI is analyzing your preferences..."):
                # Simulate some processing time for effect
                progress_bar = st.progress(0)
                for i in range(100):
                    time.sleep(0.01)
                    progress_bar.progress(i + 1)
                
                # Apply min_calories only if greater than 0
                min_cal = min_calories if min_calories > 0 else None
                max_cal = max_calories if max_calories > 0 else None
                
                # Get recipe recommendations
                recipes_df = self.find_personalized_recipes(
                    diet_preferences=selected_diets,
                    allergies=selected_allergies,
                    meal_type=selected_meal_type if selected_meal_type != "All Types" else None,
                    min_calories=min_cal,
                    max_calories=max_cal,
                    limit=num_recommendations
                )
                
                progress_bar.empty()
                
                if recipes_df.empty:
                    st.markdown("""
                    <div class="warning-box">
                        <h3>😕 No Perfect Matches Found</h3>
                        <p>Try adjusting your criteria:</p>
                        <ul>
                            <li>Remove some dietary restrictions</li>
                            <li>Increase calorie range</li>
                            <li>Try different meal types</li>
                        </ul>
                    </div>
                    """, unsafe_allow_html=True)
                else:
                    st.markdown(f"""
                    <div class="success-box">
                        <h3>🎉 Found {len(recipes_df)} Perfect Recipes for You!</h3>
                        <p>Based on your preferences, here are our top recommendations:</p>
                    </div>
                    """, unsafe_allow_html=True)
                    
                    # Store recipes in session state
                    st.session_state.current_recommendations = recipes_df
                    
                    # Display recipes with enhanced cards
                    for i, (_, row) in enumerate(recipes_df.iterrows()):
                        self._render_recipe_card(row, i, "recommendation")
    
    def _render_search_tab(self):
        """Render the recipe search tab."""
        st.header("🔍 Advanced Recipe Search")
        
        # Search interface
        col1, col2 = st.columns([3, 1])
        with col1:
            search_term = st.text_input(
                "🔍 Search for recipes by name:",
                placeholder="Try 'chocolate cake', 'chicken soup', or 'vegan pasta'...",
                help="Enter any recipe name or keyword"
            )
        with col2:
            search_limit = st.number_input("Results", min_value=1, max_value=50, value=10)
        
        if search_term:
            with st.spinner(f"Searching for '{search_term}'..."):
                search_results = self.search_recipes_by_name(search_term, search_limit)
                
                if search_results.empty:
                    st.warning(f"No recipes found containing '{search_term}'. Try different keywords!")
                else:
                    st.success(f"Found {len(search_results)} recipes matching '{search_term}'")
                    
                    for i, (_, row) in enumerate(search_results.iterrows()):
                        self._render_recipe_card(row, i, "search")
        
        # Quick search suggestions
        if st.session_state.search_history:
            st.markdown("### 📝 Recent Searches")
            cols = st.columns(min(len(st.session_state.search_history), 4))
            for i, search in enumerate(st.session_state.search_history[-4:]):
                with cols[i]:
                    if st.button(f"🔍 {search}", key=f"quick_search_{i}"):
                        st.rerun()
    
    def _render_favorites_tab(self):
        """Render the favorites tab with enhanced features."""
        st.header("❤️ Your Recipe Collection")
        
        # Get saved recipes
        saved_recipes_df = self.get_saved_recipes(st.session_state.user_id)
        
        if saved_recipes_df.empty:
            st.markdown("""
            <div class="info-box">
                <h3>📚 Start Building Your Recipe Collection!</h3>
                <p>You haven't saved any recipes yet. Here's how to get started:</p>
                <ol>
                    <li>Go to the "Smart Recommendations" tab</li>
                    <li>Find recipes you love</li>
                    <li>Click "Save Recipe" to add them here</li>
                </ol>
                <p>Your saved recipes will appear here for easy access!</p>
            </div>
            """, unsafe_allow_html=True)
        else:
            # Collection stats
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("Total Saved", len(saved_recipes_df))
            with col2:
                avg_rating = saved_recipes_df['Rating'].mean() if 'Rating' in saved_recipes_df.columns else 0
                st.metric("Avg Rating", f"{avg_rating:.1f} ⭐")
            with col3:
                if 'Calories' in saved_recipes_df.columns:
                    avg_calories = saved_recipes_df['Calories'].mean()
                    st.metric("Avg Calories", f"{avg_calories:.0f}")
            
            st.markdown("### 🍽️ Your Saved Recipes")
            
            # Display saved recipes with enhanced cards
            for i, (_, row) in enumerate(saved_recipes_df.iterrows()):
                self._render_recipe_card(row, i, "favorite", show_rating=True)
    
    def _render_analytics_tab(self):
        """Render the analytics and insights tab."""
        st.header("📊 Recipe & Nutrition Analytics")
        
        # Get analytics data
        analytics = self.get_recipe_analytics()
        
        if not analytics:
            st.error("Unable to load analytics data. Please check your database connection.")
            return
        
        # Detailed metrics
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("### 📈 Database Overview")
            calorie_stats = analytics.get('calorie_stats', {})
            
            if calorie_stats:
                st.write(f"**Average Calories per Recipe:** {calorie_stats.get('avg_calories', 0):.0f}")
                st.write(f"**Lowest Calorie Recipe:** {calorie_stats.get('min_calories', 0):.0f} calories")
                st.write(f"**Highest Calorie Recipe:** {calorie_stats.get('max_calories', 0):.0f} calories")
                st.write(f"**Recipes with Calorie Data:** {calorie_stats.get('recipes_with_calories', 0)}")
        
        with col2:
            st.markdown("### 🎯 Your Activity")
            st.write(f"**Favorite Recipes:** {len(st.session_state.favorite_recipes)}")
            st.write(f"**Recent Searches:** {len(st.session_state.search_history)}")
            if st.session_state.search_history:
                st.write(f"**Last Search:** {st.session_state.search_history[-1]}")
        
        # Visualizations
        st.markdown("### 📊 Data Visualizations")
        
        viz_col1, viz_col2 = st.columns(2)
        
        with viz_col1:
            # Calorie distribution chart
            calorie_chart = self.create_calorie_distribution_chart()
            if calorie_chart.data:
                st.plotly_chart(calorie_chart, use_container_width=True)
        
        with viz_col2:
            # Meal type distribution
            meal_chart = self.create_meal_type_chart()
            if meal_chart.data:
                st.plotly_chart(meal_chart, use_container_width=True)
        
        # Ingredient popularity chart
        st.markdown("### 🥕 Ingredient Popularity")
        ingredient_chart = self.create_ingredient_popularity_chart()
        if ingredient_chart.data:
            st.plotly_chart(ingredient_chart, use_container_width=True)
    
    def _render_ingredient_insights_tab(self):
        """Render the ingredient insights and exploration tab."""
        st.header("🧪 Ingredient Intelligence")
        
        # Ingredient search
        col1, col2 = st.columns([3, 1])
        with col1:
            ingredient_name = st.text_input(
                "🔍 Explore an ingredient:",
                placeholder="Try 'tomato', 'chicken', 'flour'...",
                help="Get detailed insights about any ingredient"
            )
        
        if ingredient_name:
            with st.spinner(f"Analyzing '{ingredient_name}'..."):
                insights = self.get_ingredient_insights(ingredient_name)
                
                if insights.get('recipe_count', 0) > 0:
                    # Display insights
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.markdown(f"""
                        <div class="metric-card">
                            <h3 style="color: #667eea;">📊 Recipe Usage</h3>
                            <h1 style="color: #333;">{insights['recipe_count']}</h1>
                            <p>recipes contain {ingredient_name}</p>
                        </div>
                        """, unsafe_allow_html=True)
                    
                    with col2:
                        allergies = insights.get('related_allergies', [])
                        if allergies:
                            allergy_list = ", ".join(allergies)
                            st.markdown(f"""
                            <div class="metric-card">
                                <h3 style="color: #e74c3c;">⚠️ Related Allergies</h3>
                                <p>{allergy_list}</p>
                            </div>
                            """, unsafe_allow_html=True)
                        else:
                            st.markdown(f"""
                            <div class="metric-card">
                                <h3 style="color: #27ae60;">✅ Allergy Status</h3>
                                <p>No known allergies associated</p>
                            </div>
                            """, unsafe_allow_html=True)
                    
                    # Find recipes with this ingredient
                    ingredient_recipes = self.find_recipes_with_ingredient(ingredient_name)
                    if not ingredient_recipes.empty:
                        st.markdown(f"### 🍽️ Recipes containing {ingredient_name}")
                        for i, (_, row) in enumerate(ingredient_recipes.head(5).iterrows()):
                            self._render_recipe_card(row, i, "ingredient")
                else:
                    st.warning(f"No data found for ingredient '{ingredient_name}'. Try a different ingredient!")
        
        # Popular ingredients section
        st.markdown("### 🏆 Most Popular Ingredients")
        analytics = self.get_recipe_analytics()
        popular_ingredients = analytics.get('popular_ingredients', pd.DataFrame())
        
        if not popular_ingredients.empty:
            # Create interactive ingredient grid
            cols = st.columns(4)
            for i, (_, row) in enumerate(popular_ingredients.head(12).iterrows()):
                with cols[i % 4]:
                    if st.button(f"🥕 {row['Ingredient']} ({row['RecipeCount']})", 
                                key=f"popular_ingredient_{i}",
                                help=f"Used in {row['RecipeCount']} recipes"):
                        # Set the ingredient in the search box (this would require a rerun to show)
                        st.session_state.selected_ingredient = row['Ingredient']
    
    def find_recipes_with_ingredient(self, ingredient_name: str, limit: int = 10) -> pd.DataFrame:
        """Find recipes containing a specific ingredient."""
        if not st.session_state.connected:
            return pd.DataFrame()
        
        query = f"""
        MATCH (i:Ingredient {{name: $ingredient_name}})<-[:CONTAINS]-(r:Recipe)
        RETURN r.name AS Recipe, r.calories AS Calories, 
               r.preparation_description AS Preparation
        ORDER BY r.calories ASC
        LIMIT {limit}
        """
        
        return st.session_state.connection.execute_query_to_df(query, {"ingredient_name": ingredient_name})
    
    def _render_recipe_card(self, recipe_row, index: int, source: str, show_rating: bool = False):
        """Render an enhanced recipe card with modern styling and detailed information."""
        recipe_name = recipe_row['Recipe']
        calories = recipe_row.get('Calories', 'N/A')
        
        # Get additional recipe information
        ingredients = self.find_recipe_ingredients(recipe_name)
        ingredient_count = len(ingredients)
        complexity = self.get_recipe_complexity_score(recipe_name)
        nutrition = self.get_recipe_nutrition_profile(recipe_name)
        
        # Create the enhanced card
        with st.expander(
            f"🍽️ {recipe_name} • {calories} cal • {ingredient_count} ingredients • {complexity.get('complexity', 'Unknown')} complexity", 
            expanded=False
        ):
            # Main content layout
            col1, col2, col3 = st.columns([2, 1, 1])
            
            with col1:
                # Ingredients section with better organization
                if ingredients:
                    st.markdown("**🥗 Ingredients:**")
                    
                    # Create ingredient tags
                    ingredient_html = ""
                    for ingredient in ingredients[:12]:  # Show max 12 ingredients
                        ingredient_html += f"""
                        <span style="background: rgba(102, 126, 234, 0.2); color: #667eea; 
                                   padding: 0.3rem 0.6rem; border-radius: 15px; margin: 0.2rem; 
                                   display: inline-block; font-size: 0.85rem;">
                            {ingredient}
                        </span>
                        """
                    
                    st.markdown(ingredient_html, unsafe_allow_html=True)
                    
                    if len(ingredients) > 12:
                        st.markdown(f"*...and {len(ingredients) - 12} more ingredients*")
                else:
                    st.markdown("*No ingredient information available*")
                
                # Preparation section
                if 'Preparation' in recipe_row and pd.notna(recipe_row['Preparation']):
                    st.markdown("**📝 Preparation:**")
                    prep_text = recipe_row['Preparation']
                    if len(prep_text) > 300:
                        prep_text = prep_text[:300] + "..."
                    st.markdown(f"_{prep_text}_")
            
            with col2:
                # Nutritional information
                st.markdown("**🔥 Nutrition:**")
                
                if nutrition:
                    # Create mini nutrition facts
                    nutrition_html = f"""
                    <div style="background: rgba(255,255,255,0.1); padding: 0.8rem; border-radius: 8px;">
                        <div style="margin: 0.3rem 0;"><strong>Calories:</strong> {nutrition.get('calories', 'N/A')}</div>
                        <div style="margin: 0.3rem 0;"><strong>Protein:</strong> {nutrition.get('protein', 'N/A')}g</div>
                        <div style="margin: 0.3rem 0;"><strong>Carbs:</strong> {nutrition.get('carbs', 'N/A')}g</div>
                        <div style="margin: 0.3rem 0;"><strong>Fat:</strong> {nutrition.get('fat', 'N/A')}g</div>
                    </div>
                    """
                    st.markdown(nutrition_html, unsafe_allow_html=True)
                else:
                    # Basic calorie display with visual indicator
                    if calories != 'N/A':
                        calorie_color = "#27ae60" if calories < 300 else "#f39c12" if calories < 600 else "#e74c3c"
                        st.markdown(f"""
                        <div style="background: {calorie_color}; color: white; padding: 0.8rem; 
                                   border-radius: 8px; text-align: center;">
                            <strong>🔥 {calories} calories</strong>
                        </div>
                        """, unsafe_allow_html=True)
                    else:
                        st.markdown("*Nutrition data not available*")
                
                # Complexity indicator
                if complexity:
                    complexity_color = {
                        'Simple': '#27ae60',
                        'Moderate': '#f39c12', 
                        'Complex': '#e74c3c'
                    }.get(complexity.get('complexity', 'Unknown'), '#95a5a6')
                    
                    st.markdown(f"""
                    <div style="background: {complexity_color}; color: white; padding: 0.5rem; 
                               border-radius: 5px; text-align: center; margin-top: 0.5rem;">
                        <strong>{complexity.get('complexity', 'Unknown')}</strong><br>
                        <small>{complexity.get('ingredient_count', 0)} ingredients</small>
                    </div>
                    """, unsafe_allow_html=True)
            
            with col3:
                # Actions and ratings
                if show_rating and 'Rating' in recipe_row:
                    st.markdown("**⭐ Your Rating:**")
                    stars = "⭐" * int(recipe_row['Rating'])
                    st.markdown(f"<h3 style='margin: 0; color: #f39c12;'>{stars}</h3>", unsafe_allow_html=True)
                
                # Action buttons with better styling
                if source != "favorite":
                    st.markdown("**💖 Save Recipe:**")
                    rating = st.selectbox(
                        "Rating", 
                        options=[1, 2, 3, 4, 5],
                        index=4,
                        key=f"rating_{source}_{index}",
                        help="Rate this recipe from 1-5 stars"
                    )
                    
                    if st.button(f"💖 Save", key=f"save_{source}_{index}", use_container_width=True):
                        if self.save_recipe_preference(st.session_state.user_id, recipe_name, rating):
                            st.success(f"✅ Saved {recipe_name}!")
                            time.sleep(1)
                            st.rerun()
                        else:
                            st.error("Failed to save recipe.")
                
                # Find similar recipes button
                if st.button(f"🔍 Similar", key=f"similar_{source}_{index}", use_container_width=True):
                    # Store the recipe for similarity search
                    st.session_state.similarity_search_recipe = recipe_name
                    st.info(f"🔍 Finding recipes similar to {recipe_name}...")
                    
                    # Get similar recipes
                    similar_recipes = self.get_recipe_recommendations_by_similarity(recipe_name, 3)
                    if not similar_recipes.empty:
                        st.markdown("**Similar recipes:**")
                        for _, similar in similar_recipes.iterrows():
                            st.markdown(f"• {similar['Recipe']} ({similar['Shared_Ingredients']} shared)")
                    else:
                        st.markdown("*No similar recipes found*")
                
                # Add nutritional analysis for comparison
                if nutrition and source != "comparison":
                    if st.button(f"� Analyze", key=f"analyze_{source}_{index}", use_container_width=True):
                        # Show detailed nutritional breakdown
                        st.markdown("**📊 Detailed Nutrition:**")
                        for nutrient, value in nutrition.items():
                            if value is not None and nutrient != 'Recipe':
                                st.markdown(f"• **{nutrient.title()}:** {value}")
        
        # Add some spacing between cards
        st.markdown("<br>", unsafe_allow_html=True)
    
    def get_recipe_nutrition_profile(self, recipe_name: str) -> Dict[str, Any]:
        """Get detailed nutritional profile for a recipe."""
        if not st.session_state.connected:
            return {}
        
        try:
            query = """
            MATCH (r:Recipe {name: $recipe_name})
            RETURN r.calories AS calories, 
                   r.protein AS protein,
                   r.fat AS fat, 
                   r.carbohydrates AS carbs,
                   r.fiber AS fiber,
                   r.sugar AS sugar,
                   r.sodium AS sodium
            """
            
            df = st.session_state.connection.execute_query_to_df(query, {"recipe_name": recipe_name})
            
            if not df.empty:
                return df.iloc[0].to_dict()
            return {}
        except Exception as e:
            st.error(f"Error getting nutrition profile: {e}")
            return {}
    
    def generate_meal_plan(self, days: int = 7, meals_per_day: int = 3) -> pd.DataFrame:
        """Generate a balanced meal plan."""
        if not st.session_state.connected:
            return pd.DataFrame()
        
        try:
            meal_types = ["Breakfast", "Lunch", "Dinner"][:meals_per_day]
            meal_plan = []
            
            for day in range(1, days + 1):
                for meal_type in meal_types:
                    # Get a random recipe for each meal type
                    query = f"""
                    MATCH (r:Recipe)-[:IS_TYPE]->(m:MealType {{name: $meal_type}})
                    WITH r, rand() AS random
                    ORDER BY random
                    RETURN r.name AS Recipe, r.calories AS Calories
                    LIMIT 1
                    """
                    
                    recipe_df = st.session_state.connection.execute_query_to_df(
                        query, {"meal_type": meal_type}
                    )
                    
                    if not recipe_df.empty:
                        meal_plan.append({
                            'Day': f"Day {day}",
                            'Meal_Type': meal_type,
                            'Recipe': recipe_df.iloc[0]['Recipe'],
                            'Calories': recipe_df.iloc[0]['Calories']
                        })
            
            return pd.DataFrame(meal_plan)
        except Exception as e:
            st.error(f"Error generating meal plan: {e}")
            return pd.DataFrame()
    
    def compare_recipes(self, recipe_names: List[str]) -> pd.DataFrame:
        """Compare nutritional profiles of multiple recipes."""
        if not st.session_state.connected or len(recipe_names) < 2:
            return pd.DataFrame()
        
        try:
            comparison_data = []
            
            for recipe_name in recipe_names:
                nutrition = self.get_recipe_nutrition_profile(recipe_name)
                if nutrition:
                    nutrition['Recipe'] = recipe_name
                    comparison_data.append(nutrition)
            
            return pd.DataFrame(comparison_data)
        except Exception as e:
            st.error(f"Error comparing recipes: {e}")
            return pd.DataFrame()
    
    def get_recipe_recommendations_by_similarity(self, base_recipe: str, limit: int = 5) -> pd.DataFrame:
        """Find recipes similar to a given recipe based on shared ingredients."""
        if not st.session_state.connected:
            return pd.DataFrame()
        
        try:
            query = f"""
            MATCH (base:Recipe {{name: $base_recipe}})-[:CONTAINS]->(shared:Ingredient)<-[:CONTAINS]-(similar:Recipe)
            WHERE base <> similar
            WITH similar, count(shared) AS shared_ingredients
            ORDER BY shared_ingredients DESC
            RETURN similar.name AS Recipe, 
                   similar.calories AS Calories,
                   shared_ingredients AS Shared_Ingredients
            LIMIT {limit}
            """
            
            return st.session_state.connection.execute_query_to_df(query, {"base_recipe": base_recipe})
        except Exception as e:
            st.error(f"Error finding similar recipes: {e}")
            return pd.DataFrame()
    
    def get_recipe_complexity_score(self, recipe_name: str) -> Dict[str, Any]:
        """Calculate a complexity score for a recipe based on ingredients and preparation."""
        if not st.session_state.connected:
            return {}
        
        try:
            # Count ingredients
            ingredients_query = """
            MATCH (r:Recipe {name: $recipe_name})-[:CONTAINS]->(i:Ingredient)
            RETURN count(i) AS ingredient_count
            """
            
            ingredients_df = st.session_state.connection.execute_query_to_df(
                ingredients_query, {"recipe_name": recipe_name}
            )
            
            ingredient_count = ingredients_df.iloc[0]['ingredient_count'] if not ingredients_df.empty else 0
            
            # Simple complexity scoring
            if ingredient_count <= 5:
                complexity = "Simple"
                score = 1
            elif ingredient_count <= 10:
                complexity = "Moderate"
                score = 2
            else:
                complexity = "Complex"
                score = 3
            
            return {
                'ingredient_count': ingredient_count,
                'complexity': complexity,
                'score': score
            }
        except Exception as e:
            st.error(f"Error calculating complexity: {e}")
            return {}
    
    def create_nutrition_radar_chart(self, recipes: List[str]) -> go.Figure:
        """Create a radar chart comparing nutritional profiles of recipes."""
        if not recipes or not st.session_state.connected:
            return go.Figure()
        
        try:
            fig = go.Figure()
            
            nutrition_categories = ['calories', 'protein', 'fat', 'carbs', 'fiber']
            
            for recipe in recipes[:3]:  # Limit to 3 recipes for clarity
                nutrition = self.get_recipe_nutrition_profile(recipe)
                
                if nutrition:
                    values = []
                    for category in nutrition_categories:
                        value = nutrition.get(category, 0)
                        # Normalize values for better visualization
                        if category == 'calories':
                            values.append(min(value / 10, 100))  # Scale calories
                        else:
                            values.append(min(value or 0, 100))  # Cap other values at 100
                    
                    fig.add_trace(go.Scatterpolar(
                        r=values,
                        theta=nutrition_categories,
                        fill='toself',
                        name=recipe[:20] + "..." if len(recipe) > 20 else recipe
                    ))
            
            fig.update_layout(            polar=dict(
                radialaxis=dict(
                    visible=True,
                    range=[0, 100]
                )),
            showlegend=True,
            title="Nutritional Profile Comparison",
            plot_bgcolor='white',
            paper_bgcolor='white',
            font=dict(color='black'),
            title_font_color='black'
            )
            
            return fig
        except Exception as e:
            st.error(f"Error creating radar chart: {e}")
            return go.Figure()
    
    def _render_meal_planning_tab(self):
        """Render the meal planning tab with weekly meal plans."""
        st.header("📅 Smart Meal Planning")
        
        # Meal planning configuration
        col1, col2, col3 = st.columns(3)
        
        with col1:
            planning_days = st.slider(
                "📆 Planning Period (days)",
                min_value=1,
                max_value=14,
                value=7,
                help="How many days do you want to plan for?"
            )
        
        with col2:
            meals_per_day = st.selectbox(
                "🍽️ Meals per Day",
                options=[1, 2, 3, 4],
                index=2,
                help="How many meals do you want planned each day?"
            )
        
        with col3:
            target_calories = st.number_input(
                "🎯 Daily Calorie Target",
                min_value=1000,
                max_value=4000,
                value=2000,
                step=100,
                help="Your daily calorie goal"
            )
        
        # Generate meal plan button
        if st.button("🤖 Generate Smart Meal Plan", use_container_width=True, type="primary"):
            with st.spinner("Creating your personalized meal plan..."):
                meal_plan_df = self.generate_meal_plan(planning_days, meals_per_day)
                
                if meal_plan_df.empty:
                    st.error("Unable to generate meal plan. Please check your database connection.")
                    return
                
                # Store meal plan in session state
                st.session_state.current_meal_plan = meal_plan_df
                
                st.success(f"✅ Generated {planning_days}-day meal plan with {len(meal_plan_df)} meals!")
        
        # Display current meal plan
        if 'current_meal_plan' in st.session_state and not st.session_state.current_meal_plan.empty:
            meal_plan_df = st.session_state.current_meal_plan
            
            # Calculate daily calories
            daily_calories = meal_plan_df.groupby('Day')['Calories'].sum().reset_index()
            avg_daily_calories = daily_calories['Calories'].mean()
            
            # Meal plan overview
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("📊 Avg Daily Calories", f"{avg_daily_calories:.0f}")
            with col2:
                st.metric("🍽️ Total Meals", len(meal_plan_df))
            with col3:
                calories_diff = avg_daily_calories - target_calories
                st.metric("🎯 vs Target", f"{calories_diff:+.0f} cal")
            
            # Display meal plan in an organized way
            st.markdown("### 📋 Your Meal Plan")
            
            for day in meal_plan_df['Day'].unique():
                with st.expander(f"📅 {day} - {meal_plan_df[meal_plan_df['Day'] == day]['Calories'].sum():.0f} calories", expanded=False):
                    day_meals = meal_plan_df[meal_plan_df['Day'] == day]
                    
                    cols = st.columns(len(day_meals))
                    for i, (_, meal) in enumerate(day_meals.iterrows()):
                        with cols[i]:
                            st.markdown(f"""
                            <div class="recipe-card" style="margin: 0.5rem; padding: 1rem;">
                                <h4>{meal['Meal_Type']}</h4>
                                <h5>{meal['Recipe']}</h5>
                                <p>🔥 {meal['Calories']} calories</p>
                            </div>
                            """, unsafe_allow_html=True)
            
            # Download meal plan option
            if st.button("📥 Export Meal Plan", help="Download your meal plan as CSV"):
                csv = meal_plan_df.to_csv(index=False)
                st.download_button(
                    label="📥 Download CSV",
                    data=csv,
                    file_name=f"meal_plan_{planning_days}_days.csv",
                    mime="text/csv"
                )
    
    def _render_recipe_comparison_tab(self):
        """Render the recipe comparison tab."""
        st.header("⚖️ Recipe Comparison & Analysis")
        
        # Recipe selection for comparison
        st.markdown("### 🔍 Select Recipes to Compare")
        
        col1, col2 = st.columns(2)
        
        with col1:
            search_for_comparison = st.text_input(
                "🔍 Search recipes to compare:",
                placeholder="Search for recipes...",
                key="comparison_search"
            )
            
            if search_for_comparison:
                search_results = self.search_recipes_by_name(search_for_comparison, 5)
                if not search_results.empty:
                    st.markdown("**Search Results:**")
                    for _, recipe in search_results.iterrows():
                        if st.button(f"➕ Add {recipe['Recipe']}", key=f"add_comparison_{recipe['Recipe']}"):
                            if 'comparison_recipes' not in st.session_state:
                                st.session_state.comparison_recipes = []
                            if recipe['Recipe'] not in st.session_state.comparison_recipes:
                                st.session_state.comparison_recipes.append(recipe['Recipe'])
                                st.rerun()
        
        with col2:
            # Selected recipes for comparison
            if 'comparison_recipes' in st.session_state and st.session_state.comparison_recipes:
                st.markdown("**Selected for Comparison:**")
                for i, recipe in enumerate(st.session_state.comparison_recipes):
                    col_a, col_b = st.columns([3, 1])
                    with col_a:
                        st.write(f"• {recipe}")
                    with col_b:
                        if st.button("❌", key=f"remove_comparison_{i}"):
                            st.session_state.comparison_recipes.remove(recipe)
                            st.rerun()
                
                if len(st.session_state.comparison_recipes) >= 2:
                    if st.button("📊 Compare Recipes", use_container_width=True, type="primary"):
                        # Perform comparison
                        comparison_df = self.compare_recipes(st.session_state.comparison_recipes)
                        
                        if not comparison_df.empty:
                            st.markdown("### 📊 Nutritional Comparison")
                            
                            # Display comparison table
                            st.dataframe(comparison_df.set_index('Recipe'), use_container_width=True)
                            
                            # Radar chart comparison
                            if len(st.session_state.comparison_recipes) <= 3:
                                st.markdown("### 🕸️ Nutritional Profile Radar")
                                radar_chart = self.create_nutrition_radar_chart(st.session_state.comparison_recipes)
                                if radar_chart.data:
                                    st.plotly_chart(radar_chart, use_container_width=True)
                            
                            # Complexity comparison
                            st.markdown("### 🎯 Recipe Complexity Analysis")
                            complexity_data = []
                            for recipe in st.session_state.comparison_recipes:
                                complexity = self.get_recipe_complexity_score(recipe)
                                complexity['Recipe'] = recipe
                                complexity_data.append(complexity)
                            
                            if complexity_data:
                                complexity_df = pd.DataFrame(complexity_data)
                                
                                # Complexity bar chart
                                fig = px.bar(
                                    complexity_df,
                                    x='Recipe',
                                    y='ingredient_count',
                                    color='complexity',
                                    title='Recipe Complexity by Ingredient Count',
                                    labels={'ingredient_count': 'Number of Ingredients'},
                                    color_discrete_map={
                                        'Simple': '#27ae60',
                                        'Moderate': '#f39c12', 
                                        'Complex': '#e74c3c'
                                    }
                                )
                                
                                fig.update_layout(
                                    plot_bgcolor='white',
                                    paper_bgcolor='white',
                                    font=dict(color='black'),
                                    title_font_color='black'
                                )
                                
                                st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("Search and select at least 2 recipes to compare their nutritional profiles.")
        
        # Similar recipes feature
        st.markdown("### 🔍 Find Similar Recipes")
        base_recipe_for_similarity = st.text_input(
            "Enter a recipe name to find similar recipes:",
            placeholder="e.g., 'Chicken Parmesan'",
            key="similarity_search"
        )
        
        if base_recipe_for_similarity:
            similar_recipes = self.get_recipe_recommendations_by_similarity(base_recipe_for_similarity, 5)
            
            if not similar_recipes.empty:
                st.markdown(f"**Recipes similar to '{base_recipe_for_similarity}':**")
                
                for _, recipe in similar_recipes.iterrows():
                    with st.expander(f"🍽️ {recipe['Recipe']} - {recipe['Shared_Ingredients']} shared ingredients"):
                        col1, col2 = st.columns(2)
                        
                        with col1:
                            st.write(f"**Calories:** {recipe['Calories']}")
                            st.write(f"**Shared Ingredients:** {recipe['Shared_Ingredients']}")
                        
                        with col2:
                            if st.button(f"🔗 Add to Comparison", key=f"add_similar_{recipe['Recipe']}"):
                                if 'comparison_recipes' not in st.session_state:
                                    st.session_state.comparison_recipes = []
                                if recipe['Recipe'] not in st.session_state.comparison_recipes:
                                    st.session_state.comparison_recipes.append(recipe['Recipe'])
                                    st.success(f"Added {recipe['Recipe']} to comparison!")
                                    time.sleep(1)
                                    st.rerun()
            else:
                st.warning(f"No similar recipes found for '{base_recipe_for_similarity}'. Try a different recipe name.")