import streamlit as st
from typing import Dict, Any, List
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px


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
    
    




def get_recipe_analytics() -> Dict[str, Any]:
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
        
def search_recipes_by_name(search_term: str, limit: int = 10) -> pd.DataFrame:
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

def get_ingredient_insights(ingredient_name: str) -> Dict[str, Any]:
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
            
            
            