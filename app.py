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
from dashboard.dashboard_helpers import apply_custom_css
from dashboard.dashboard_helpers import get_recipe_analytics


from dashboard.visualization import display_metrics_dashboard
from dashboard.database_init import connect_to_database


from dashboard.pages.meal_planning import render_meal_planning_tab
from dashboard.pages.analytics import render_analytics_tab
from dashboard.pages.ingredient_insights import render_ingredient_insights_tab
from dashboard.pages.comparison import render_recipe_comparison_tab
from dashboard.pages.favorites import render_favorites_tab
from dashboard.pages.recipe_search import render_search_tab
from dashboard.pages.recommendations import render_recommendations_tab



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



# Create a class for our recipe recommendation dashboard
class SmartRecipeRecommendationDashboard:
    """
    An advanced Streamlit dashboard providing intelligent recipe recommendations,
    nutritional insights, and comprehensive food analytics with modern UI.
    """
    
    def __init__(self):
        """Initialize the dashboard with database connection and enhanced session state."""
        # Initialize session state variables if they don't exist
        defaults = {
            'connected': False,
            'connection': None,
            'query_manager': None,
            'diet_preferences': [],
            'allergies': [],
            'meal_types': [],
            'favorite_recipes': [],
            'user_id': f"user_{np.random.randint(10000)}",
            'search_history': [],
            'recipe_analytics': {},
        }

        for key, value in defaults.items():
            st.session_state.setdefault(key, value)
        
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
                        if connect_to_database():
                            st.success("✅ Connected successfully!")
                            time.sleep(1)
                            st.rerun()
                        else:
                            st.error("❌ Connection failed.")
            else:
                st.success("✅ Connected to Neo4j Database")
                
                # Quick stats in sidebar
                st.markdown("### 📊 Quick Stats")
                analytics = get_recipe_analytics()
                
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
        display_metrics_dashboard()
        
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
            render_recommendations_tab()
        
        with tab2:
            render_search_tab()
        
        with tab3:
            render_favorites_tab()
        
        with tab4:
            render_analytics_tab()
        
        with tab5:
            render_ingredient_insights_tab()
        
        with tab6:
            render_meal_planning_tab()
        
        with tab7:
            render_recipe_comparison_tab()
    

if __name__ == "__main__":
    dashboard = SmartRecipeRecommendationDashboard()
    dashboard.run()