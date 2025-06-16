import streamlit as st
from src.graph_db.neo4j.connection import Neo4jConnection
from src.graph_db.queries.manager import QueryManager
import pandas as pd


def connect_to_database(uri="bolt://localhost:7687", user="neo4j", password="password") -> bool:
    """Establish connection to the Neo4j database and load config values into session state."""
    if st.session_state.get("connected", False):
        return True

    try:
        connection = Neo4jConnection(uri=uri, user=user, password=password)
        if connection.connect():
            st.session_state.connection = connection
            st.session_state.query_manager = QueryManager(connection.get_driver())
            st.session_state.connected = True

            load_diet_preferences()
            load_allergies()
            load_meal_types()
            return True
        return False
    except Exception as e:
        st.error(f"Failed to connect to database: {e}")
        return False


def load_diet_preferences():
    """Load available diet preferences into session state."""
    if not st.session_state.connected:
        return

    query = """
    MATCH (d:DietPreference)
    RETURN DISTINCT d.name AS DietPreference
    ORDER BY d.name
    """

    try:
        df = st.session_state.connection.execute_query_to_df(query, {})
        st.session_state.diet_preferences = (
            df["DietPreference"].tolist() if not df.empty else [
                "Vegetarian", "Vegan", "Gluten-Free", 
                "Dairy-Free", "Nut-Free", "Pescatarian",
                "Keto", "Paleo", "Mediterranean"
            ]
        )
        if df.empty:
            st.sidebar.info("Loading default diet preferences - they'll be available after data loading")
    except Exception as e:
        st.error(f"Error loading diet preferences: {e}")
        st.session_state.diet_preferences = [
            "Vegetarian", "Vegan", "Gluten-Free", 
            "Dairy-Free", "Nut-Free", "Pescatarian",
            "Keto", "Paleo", "Mediterranean"
        ]


def load_allergies():
    """Load available allergies into session state."""
    if not st.session_state.connected:
        return

    query = """
    MATCH (a:Allergy)
    RETURN DISTINCT a.name AS Allergen
    ORDER BY a.name
    LIMIT 50
    """

    try:
        df = st.session_state.connection.execute_query_to_df(query, {})
        st.session_state.allergies = (
            df["Allergen"].tolist() if not df.empty else [
                # Common allergens that match our ingredient classification
                "Nuts", "Peanuts", "Fish", "Shellfish", "Seafood",
                "Eggs", "Dairy", "Milk", "Soy", "Gluten", 
                "Wheat", "Sesame", "Tree Nuts"
            ]
        )
        if df.empty:
            st.sidebar.info("Loading default allergens - they'll be available after data loading")
    except Exception as e:
        st.error(f"Error loading allergies: {e}")
        st.session_state.allergies = [
            "Nuts", "Peanuts", "Fish", "Shellfish", "Seafood",
            "Eggs", "Dairy", "Milk", "Soy", "Gluten", 
            "Wheat", "Sesame", "Tree Nuts"
        ]


def load_meal_types():
    """Load available meal types into session state."""
    if not st.session_state.connected:
        return

    query = """
    MATCH (m:MealType)
    RETURN DISTINCT m.name AS MealType
    ORDER BY m.name
    """

    try:
        df = st.session_state.connection.execute_query_to_df(query, {})
        st.session_state.meal_types = (
            df["MealType"].tolist() if not df.empty else [
                "Breakfast", "Lunch", "Dinner", "Drink", "Other"
            ]
        )
        if df.empty:
            st.sidebar.warning("No meal types found in database, using defaults")
    except Exception as e:
        st.error(f"Error loading meal types: {e}")
        st.session_state.meal_types = [
            "Breakfast", "Lunch", "Dinner", "Drink", "Other"
        ]
