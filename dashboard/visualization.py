import streamlit as st
import pandas as pd
from typing import List, Dict, Any
import plotly.express as px
import plotly.graph_objects as go

from dashboard.dashboard_helpers import get_recipe_analytics
from dashboard.queries import (
    get_recipe_nutrition_profile,
)

def display_metrics_dashboard():
    """Display key metrics in an attractive dashboard format with enhanced styling."""
    analytics = get_recipe_analytics()
    
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

def create_calorie_distribution_chart() -> go.Figure:
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

def create_meal_type_chart() -> go.Figure:
    """Create a pie chart of meal type distribution."""
    analytics = get_recipe_analytics()
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

def create_ingredient_popularity_chart() -> go.Figure:
    """Create a bar chart of most popular ingredients."""
    analytics = get_recipe_analytics()
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

def create_nutrition_radar_chart(recipes: List[str]) -> go.Figure:
    """Create a radar chart comparing nutritional profiles of recipes."""
    if not recipes or not st.session_state.connected:
        return go.Figure()
    
    try:
        fig = go.Figure()
        nutrition_categories = ['calories', 'protein', 'fat', 'carbs', 'fiber']

        for recipe in recipes[:3]:  # Limit to 3 recipes for clarity
            nutrition = get_recipe_nutrition_profile(recipe)

            if nutrition:
                values = []
                for category in nutrition_categories:
                    value = nutrition.get(category, 0)
                    if category == 'calories':
                        values.append(min(value / 10, 100))
                    else:
                        values.append(min(value or 0, 100))

                fig.add_trace(go.Scatterpolar(
                    r=values,
                    theta=nutrition_categories,
                    fill='toself',
                    name=recipe[:20] + "..." if len(recipe) > 20 else recipe
                ))

        fig.update_layout(
            polar=dict(
                radialaxis=dict(visible=True, range=[0, 100])
            ),
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
    
