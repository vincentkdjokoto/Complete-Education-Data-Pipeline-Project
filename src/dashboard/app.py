#!/usr/bin/env python3
"""
Education Data Dashboard
Streamlit dashboard for exploring education statistics
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import sqlite3
from datetime import datetime
import yaml
import os

# Page configuration
st.set_page_config(
    page_title="OECD Education Data Dashboard",
    page_icon="🎓",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Load configuration
with open("config.yaml", 'r') as f:
    config = yaml.safe_load(f)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        color: #2E86AB;
        text-align: center;
        margin-bottom: 1rem;
    }
    .metric-card {
        background-color: #F8F9FA;
        padding: 15px;
        border-radius: 10px;
        margin: 10px 0;
        border-left: 5px solid #2E86AB;
    }
    .data-source {
        font-size: 0.8rem;
        color: #6c757d;
        font-style: italic;
    }
</style>
""", unsafe_allow_html=True)

class EducationDashboard:
    """Education data dashboard"""
    
    def __init__(self, db_path):
        self.db_path = db_path
        self.conn = None
    
    def connect(self):
        """Connect to SQLite database"""
        try:
            self.conn = sqlite3.connect(self.db_path)
            return True
        except Exception as e:
            st.error(f"Error connecting to database: {e}")
            return False
    
    def query_data(self, query):
        """Execute SQL query and return DataFrame"""
        try:
            df = pd.read_sql_query(query, self.conn)
            return df
        except Exception as e:
            st.error(f"Error executing query: {e}")
            return pd.DataFrame()
    
    def get_database_stats(self):
        """Get statistics about the database"""
        stats = {}
        
        tables = ['enrollment_data', 'graduation_data', 
                  'education_spending', 'country_metadata']
        
        for table in tables:
            try:
                query = f"SELECT COUNT(*) as count FROM {table}"
                df = self.query_data(query)
                stats[table] = df['count'].iloc[0]
            except:
                stats[table] = 0
        
        return stats
    
    def get_country_list(self):
        """Get list of countries in the database"""
        query = """
        SELECT DISTINCT country_code, country_name 
        FROM country_metadata 
        ORDER BY country_name
        """
        return self.query_data(query)
    
    def get_enrollment_trends(self, country_codes=None):
        """Get enrollment trends for selected countries"""
        if country_codes:
            country_filter = f"WHERE country_code IN ({','.join(['?']*len(country_codes))})"
            params = country_codes
        else:
            country_filter = ""
            params = []
        
        query = f"""
        SELECT 
            year,
            country_code,
            country_name,
            AVG(enrollment_rate) as avg_enrollment,
            COUNT(*) as data_points
        FROM enrollment_data
        {country_filter}
        GROUP BY year, country_code, country_name
        ORDER BY year, country_code
        """
        
        df = self.query_data(query)
        if not df.empty and params:
            df = df[df['country_code'].isin(params)]
        
        return df
    
    def get_graduation_rates(self, year=2022):
        """Get graduation rates for a specific year"""
        query = f"""
        SELECT 
            country_code,
            country_name,
            graduation_rate,
            completion_rate
        FROM graduation_data
        WHERE year = {year}
        ORDER BY graduation_rate DESC
        LIMIT 20
        """
        return self.query_data(query)
    
    def get_spending_comparison(self, year=2022):
        """Get education spending comparison"""
        query = f"""
        SELECT 
            country_code,
            country_name,
            spending_usd,
            spending_per_capita
        FROM education_spending
        WHERE year = {year}
        AND spending_usd IS NOT NULL
        ORDER BY spending_usd DESC
        LIMIT 15
        """
        return self.query_data(query)
    
    def get_education_indicators(self, country_code):
        """Get all education indicators for a specific country"""
        queries = {
            'enrollment': f"""
                SELECT year, enrollment_rate 
                FROM enrollment_data 
                WHERE country_code = '{country_code}'
                ORDER BY year
            """,
            'graduation': f"""
                SELECT year, graduation_rate 
                FROM graduation_data 
                WHERE country_code = '{country_code}'
                ORDER BY year
            """,
            'spending': f"""
                SELECT year, spending_usd 
                FROM education_spending 
                WHERE country_code = '{country_code}'
                ORDER BY year
            """
        }
        
        results = {}
        for key, query in queries.items():
            results[key] = self.query_data(query)
        
        return results

def main():
    """Main dashboard function"""
    st.markdown('<h1 class="main-header">🎓 OECD Education Data Dashboard</h1>', 
                unsafe_allow_html=True)
    
    # Initialize dashboard
    db_path = config['database']['sqlite_path']
    dashboard = EducationDashboard(db_path)
    
    if not dashboard.connect():
        st.error("Failed to connect to database. Please check if the ETL pipeline has been run.")
        return
    
    # Sidebar
    with st.sidebar:
        st.header("📊 Dashboard Controls")
        
        # Data source info
        st.markdown("### Data Source")
        st.markdown("""
        **OECD Education Statistics**
        - Enrollment Rates
        - Graduation Rates
        - Education Spending
        """)
        
        # Last updated
        if os.path.exists(db_path):
            mod_time = os.path.getmtime(db_path)
            st.markdown(f"**Last Updated:** {datetime.fromtimestamp(mod_time).strftime('%Y-%m-%d %H:%M')}")
        
        # Database statistics
        st.markdown("### Database Statistics")
        stats = dashboard.get_database_stats()
        
        col1, col2 = st.columns(2)
        with col1:
            st.metric("Countries", len(dashboard.get_country_list()))
            st.metric("Enrollment Records", stats.get('enrollment_data', 0))
        
        with col2:
            st.metric("Graduation Records", stats.get('graduation_data', 0))
            st.metric("Spending Records", stats.get('education_spending', 0))
        
        # Country selection
        st.markdown("### Country Selection")
        countries_df = dashboard.get_country_list()
        selected_countries = st.multiselect(
            "Select Countries",
            options=countries_df['country_name'].tolist(),
            default=['United States', 'United Kingdom', 'Germany', 'Japan']
        )
        
        # Year selection
        st.markdown("### Year Selection")
        selected_year = st.slider(
            "Select Year",
            min_value=2000,
            max_value=2023,
            value=2022,
            step=1
        )
    
    # Main content
    tab1, tab2, tab3, tab4 = st.tabs(["📈 Trends", "🎓 Graduation", "💰 Spending", "🌍 Country Profile"])
    
    with tab1:
        st.header("Enrollment Rate Trends")
        
        if selected_countries:
            # Get country codes for selected countries
            country_codes = countries_df[
                countries_df['country_name'].isin(selected_countries)
            ]['country_code'].tolist()
            
            # Get enrollment trends
            trends_df = dashboard.get_enrollment_trends(country_codes)
            
            if not trends_df.empty:
                # Line chart
                fig = px.line(
                    trends_df,
                    x='year',
                    y='avg_enrollment',
                    color='country_name',
                    title='Enrollment Rate Trends (2000-2023)',
                    labels={'avg_enrollment': 'Average Enrollment Rate (%)', 'year': 'Year'},
                    markers=True
                )
                
                fig.update_layout(
                    height=500,
                    hovermode='x unified',
                    yaxis_range=[0, 120]
                )
                
                st.plotly_chart(fig, use_container_width=True)
                
                # Data table
                with st.expander("View Data Table"):
                    st.dataframe(trends_df.pivot_table(
                        index='year',
                        columns='country_name',
                        values='avg_enrollment'
                    ).round(2))
            else:
                st.info("No enrollment data available for selected countries")
        else:
            st.info("Please select countries from the sidebar")
    
    with tab2:
        st.header(f"Graduation Rates ({selected_year})")
        
        # Get graduation rates
        grad_df = dashboard.get_graduation_rates(selected_year)
        
        if not grad_df.empty:
            col1, col2 = st.columns(2)
            
            with col1:
                # Bar chart
                fig = px.bar(
                    grad_df.head(10),
                    x='graduation_rate',
                    y='country_name',
                    orientation='h',
                    title='Top 10 Countries by Graduation Rate',
                    labels={'graduation_rate': 'Graduation Rate (%)', 'country_name': 'Country'},
                    color='graduation_rate',
                    color_continuous_scale='Viridis'
                )
                
                fig.update_layout(height=400)
                st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                # Completion rate scatter
                fig = px.scatter(
                    grad_df,
                    x='graduation_rate',
                    y='completion_rate',
                    size='graduation_rate',
                    color='country_name',
                    title='Graduation vs Completion Rates',
                    labels={
                        'graduation_rate': 'Graduation Rate (%)',
                        'completion_rate': 'Completion Rate'
                    },
                    hover_name='country_name'
                )
                
                fig.update_layout(height=400)
                st.plotly_chart(fig, use_container_width=True)
            
            # Data table
            with st.expander("View Full Data Table"):
                st.dataframe(grad_df[['country_name', 'graduation_rate', 'completion_rate']])
        else:
            st.info(f"No graduation data available for {selected_year}")
    
    with tab3:
        st.header(f"Education Spending ({selected_year})")
        
        # Get spending data
        spending_df = dashboard.get_spending_comparison(selected_year)
        
        if not spending_df.empty:
            # Create subplots
            fig = make_subplots(
                rows=1, cols=2,
                subplot_titles=('Total Education Spending', 'Spending per Capita'),
                specs=[[{'type': 'bar'}, {'type': 'bar'}]]
            )
            
            # Total spending
            fig.add_trace(
                go.Bar(
                    x=spending_df['country_name'].head(10),
                    y=spending_df['spending_usd'].head(10),
                    name='Total Spending (USD)',
                    marker_color='#2E86AB'
                ),
                row=1, col=1
            )
            
            # Per capita spending
            fig.add_trace(
                go.Bar(
                    x=spending_df['country_name'].head(10),
                    y=spending_df['spending_per_capita'].head(10),
                    name='Per Capita (USD)',
                    marker_color='#F18F01'
                ),
                row=1, col=2
            )
            
            fig.update_layout(
                height=500,
                showlegend=True,
                title_text=f"Education Spending Comparison - {selected_year}"
            )
            
            fig.update_xaxes(tickangle=45)
            
            st.plotly_chart(fig, use_container_width=True)
            
            # Spending metrics
            col1, col2, col3 = st.columns(3)
            with col1:
                avg_spending = spending_df['spending_usd'].mean()
                st.metric("Average Spending", f"${avg_spending:,.0f}")
            
            with col2:
                max_spending = spending_df['spending_usd'].max()
                max_country = spending_df.loc[spending_df['spending_usd'].idxmax(), 'country_name']
                st.metric("Highest Spending", f"${max_spending:,.0f}", f"{max_country}")
            
            with col3:
                min_spending = spending_df['spending_usd'].min()
                min_country = spending_df.loc[spending_df['spending_usd'].idxmin(), 'country_name']
                st.metric("Lowest Spending", f"${min_spending:,.0f}", f"{min_country}")
            
        else:
            st.info(f"No spending data available for {selected_year}")
    
    with tab4:
        st.header("Country Education Profile")
        
        if selected_countries:
            selected_country = selected_countries[0]
            country_code = countries_df[
                countries_df['country_name'] == selected_country
            ]['country_code'].iloc[0]
            
            # Get all indicators for selected country
            indicators = dashboard.get_education_indicators(country_code)
            
            st.subheader(f"Education Profile: {selected_country}")
            
            # Create metrics row
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                if not indicators['enrollment'].empty:
                    latest_enrollment = indicators['enrollment']['enrollment_rate'].iloc[-1]
                    st.metric("Latest Enrollment Rate", f"{latest_enrollment:.1f}%")
            
            with col2:
                if not indicators['graduation'].empty:
                    latest_graduation = indicators['graduation']['graduation_rate'].iloc[-1]
                    st.metric("Latest Graduation Rate", f"{latest_graduation:.1f}%")
            
            with col3:
                if not indicators['spending'].empty:
                    latest_spending = indicators['spending']['spending_usd'].iloc[-1]
                    st.metric("Latest Spending", f"${latest_spending:,.0f}")
            
            with col4:
                if not indicators['enrollment'].empty:
                    enrollment_trend = indicators['enrollment']['enrollment_rate'].pct_change().iloc[-1] * 100
                    st.metric("Enrollment Trend", f"{enrollment_trend:+.1f}%")
            
            # Create time series chart
            fig = go.Figure()
            
            if not indicators['enrollment'].empty:
                fig.add_trace(go.Scatter(
                    x=indicators['enrollment']['year'],
                    y=indicators['enrollment']['enrollment_rate'],
                    mode='lines+markers',
                    name='Enrollment Rate',
                    yaxis='y1'
                ))
            
            if not indicators['graduation'].empty:
                fig.add_trace(go.Scatter(
                    x=indicators['graduation']['year'],
                    y=indicators['graduation']['graduation_rate'],
                    mode='lines+markers',
                    name='Graduation Rate',
                    yaxis='y1'
                ))
            
            if not indicators['spending'].empty:
                # Add secondary axis for spending
                fig.add_trace(go.Scatter(
                    x=indicators['spending']['year'],
                    y=indicators['spending']['spending_usd'],
                    mode='lines+markers',
                    name='Spending (USD)',
                    yaxis='y2'
                ))
            
            fig.update_layout(
                title=f"Education Indicators Over Time - {selected_country}",
                height=500,
                hovermode='x unified',
                yaxis=dict(title="Rate (%)"),
                yaxis2=dict(
                    title="Spending (USD)",
                    overlaying='y',
                    side='right'
                )
            )
            
            st.plotly_chart(fig, use_container_width=True)
            
        else:
            st.info("Please select a country from the sidebar")
    
    # Footer
    st.markdown("---")
    st.markdown("""
    <div class="data-source">
    Data Source: OECD Education Statistics | Dashboard Version 1.0 | 
    <a href="https://github.com/yourusername/education-data-pipeline" target="_blank">View on GitHub</a>
    </div>
    """, unsafe_allow_html=True)

if __name__ == "__main__":
    main()
