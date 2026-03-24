"""
Streamlit Dashboard — Employee Salary Data from MySQL

Displays employee salary data loaded by the Airflow ETL pipeline.
Features: KPI cards, filters, search, charts, and raw data table.
"""

import os
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine

# ── Page config ──
st.set_page_config(
    page_title="👔 Employee Salary Dashboard",
    page_icon="👔",
    layout="wide",
)

# ── Database connection ──
is_docker = os.path.exists("/.dockerenv")
DB_HOST = os.getenv("DB_HOST", "mysql" if is_docker else "localhost")
DB_PORT = os.getenv("DB_PORT", "3306" if is_docker else "3307")

MYSQL_URL = f"mysql+pymysql://employee_user:employeepassword@{DB_HOST}:{DB_PORT}/employee_db"


@st.cache_data(ttl=300)
def load_data():
    """Load employee salary data from MySQL."""
    engine = create_engine(MYSQL_URL)
    try:
        df = pd.read_sql("SELECT * FROM employee_salary", con=engine)
        return df
    except Exception as e:
        st.error(f"Could not load data: {e}")
        return pd.DataFrame()


# ── Load data ──
df = load_data()

# ── Header ──
st.title("👔 Employee Salary Dashboard")
st.markdown("Employee salary data powered by **Airflow ETL pipeline** and **MySQL**.")

if df.empty:
    st.warning("⚠️ No employee data found. Please run the Airflow DAG (`etl_employee_salary`) first.")
    st.stop()

# ── Standardise column names for display ──
df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")

# ── Detect key columns dynamically ──
salary_col = next((c for c in df.columns if "salary" in c and "final" not in c), None)
final_salary_col = next((c for c in df.columns if "final_salary" in c), None)
bonus_col = next((c for c in df.columns if "bonus" in c), None)
dept_col = next((c for c in df.columns if "department" in c), None)
edu_col = next((c for c in df.columns if "education" in c), None)
city_col = next((c for c in df.columns if "city" in c), None)
exp_col = next((c for c in df.columns if "experience" in c or "exp" in c), None)
age_col = next((c for c in df.columns if "age" in c), None)
gender_col = next((c for c in df.columns if "gender" in c), None)

# ═══════════════════════════════════════════════════════
# Sidebar — Filters & Search
# ═══════════════════════════════════════════════════════
st.sidebar.header("🔍 Filters & Search")

# Text search
search_query = st.sidebar.text_input("🔎 Search (name, department, city…)", "")

filtered_df = df.copy()

# Apply text search across all string columns
if search_query:
    mask = pd.Series([False] * len(filtered_df))
    for col in filtered_df.select_dtypes(include=["object"]).columns:
        mask = mask | filtered_df[col].astype(str).str.contains(search_query, case=False, na=False)
    filtered_df = filtered_df[mask]

# Department filter
if dept_col and dept_col in filtered_df.columns:
    departments = sorted(filtered_df[dept_col].dropna().unique())
    selected_depts = st.sidebar.multiselect("🏢 Department", departments, default=[])
    if selected_depts:
        filtered_df = filtered_df[filtered_df[dept_col].isin(selected_depts)]

# Education filter
if edu_col and edu_col in filtered_df.columns:
    educations = sorted(filtered_df[edu_col].dropna().unique())
    selected_edu = st.sidebar.multiselect("🎓 Education Level", educations, default=[])
    if selected_edu:
        filtered_df = filtered_df[filtered_df[edu_col].isin(selected_edu)]

# City filter
if city_col and city_col in filtered_df.columns:
    cities = sorted(filtered_df[city_col].dropna().unique())
    selected_cities = st.sidebar.multiselect("🌆 City", cities, default=[])
    if selected_cities:
        filtered_df = filtered_df[filtered_df[city_col].isin(selected_cities)]

# Gender filter
if gender_col and gender_col in filtered_df.columns:
    genders = sorted(filtered_df[gender_col].dropna().unique())
    selected_genders = st.sidebar.multiselect("👤 Gender", genders, default=[])
    if selected_genders:
        filtered_df = filtered_df[filtered_df[gender_col].isin(selected_genders)]

# Experience range filter
if exp_col and exp_col in filtered_df.columns:
    exp_min = int(filtered_df[exp_col].min())
    exp_max = int(filtered_df[exp_col].max())
    if exp_min < exp_max:
        exp_range = st.sidebar.slider(
            "📊 Experience (years)", exp_min, exp_max, (exp_min, exp_max)
        )
        filtered_df = filtered_df[
            (filtered_df[exp_col] >= exp_range[0]) & (filtered_df[exp_col] <= exp_range[1])
        ]

st.sidebar.markdown(f"**Showing {len(filtered_df)} of {len(df)} employees**")

# ═══════════════════════════════════════════════════════
# KPI Cards
# ═══════════════════════════════════════════════════════
st.markdown("---")
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric("👥 Total Employees", f"{len(filtered_df):,}")
with col2:
    if salary_col:
        avg_salary = filtered_df[salary_col].mean()
        st.metric("💰 Avg Salary", f"${avg_salary:,.0f}")
with col3:
    if final_salary_col:
        avg_final = filtered_df[final_salary_col].mean()
        st.metric("💎 Avg Final Salary", f"${avg_final:,.0f}")
with col4:
    if dept_col:
        n_depts = filtered_df[dept_col].nunique()
        st.metric("🏢 Departments", n_depts)

st.markdown("---")

# ═══════════════════════════════════════════════════════
# Charts
# ═══════════════════════════════════════════════════════

# ── Row 1: Salary Distribution + Department Comparison ──
chart_col1, chart_col2 = st.columns(2)

with chart_col1:
    if salary_col:
        st.subheader("💰 Salary Distribution")
        fig_dist = px.histogram(
            filtered_df, x=salary_col, nbins=30,
            color_discrete_sequence=["#667eea"],
            labels={salary_col: "Salary"},
        )
        fig_dist.update_layout(template="plotly_dark", height=380, showlegend=False)
        st.plotly_chart(fig_dist, use_container_width=True)

with chart_col2:
    if dept_col and salary_col:
        st.subheader("🏢 Average Salary by Department")
        dept_avg = filtered_df.groupby(dept_col)[salary_col].mean().sort_values(ascending=True)
        fig_dept = px.bar(
            x=dept_avg.values, y=dept_avg.index,
            orientation="h",
            color=dept_avg.values,
            color_continuous_scale="Viridis",
            labels={"x": "Average Salary", "y": "Department"},
        )
        fig_dept.update_layout(template="plotly_dark", height=380, showlegend=False)
        st.plotly_chart(fig_dept, use_container_width=True)

# ── Row 2: Experience vs Salary + Final Salary Comparison ──
chart_col3, chart_col4 = st.columns(2)

with chart_col3:
    if exp_col and salary_col:
        st.subheader("📊 Experience vs Salary")
        color_by = dept_col if dept_col else None
        fig_scatter = px.scatter(
            filtered_df, x=exp_col, y=salary_col,
            color=color_by,
            labels={exp_col: "Experience (years)", salary_col: "Salary"},
            opacity=0.7,
        )
        fig_scatter.update_layout(template="plotly_dark", height=380)
        st.plotly_chart(fig_scatter, use_container_width=True)

with chart_col4:
    if salary_col and final_salary_col:
        st.subheader("💎 Salary vs Final Salary")
        fig_compare = go.Figure()
        fig_compare.add_trace(go.Box(
            y=filtered_df[salary_col], name="Base Salary",
            marker_color="#667eea",
        ))
        fig_compare.add_trace(go.Box(
            y=filtered_df[final_salary_col], name="Final Salary",
            marker_color="#764ba2",
        ))
        fig_compare.update_layout(template="plotly_dark", height=380)
        st.plotly_chart(fig_compare, use_container_width=True)

# ── Row 3: Bonus & Education ──
chart_col5, chart_col6 = st.columns(2)

with chart_col5:
    if bonus_col:
        st.subheader("🎁 Bonus Percentage Distribution")
        fig_bonus = px.histogram(
            filtered_df, x=bonus_col, nbins=20,
            color_discrete_sequence=["#f093fb"],
            labels={bonus_col: "Bonus (%)"},
        )
        fig_bonus.update_layout(template="plotly_dark", height=350, showlegend=False)
        st.plotly_chart(fig_bonus, use_container_width=True)

with chart_col6:
    if edu_col and salary_col:
        st.subheader("🎓 Salary by Education Level")
        fig_edu = px.box(
            filtered_df, x=edu_col, y=salary_col,
            color=edu_col,
            labels={edu_col: "Education", salary_col: "Salary"},
        )
        fig_edu.update_layout(template="plotly_dark", height=350, showlegend=False)
        st.plotly_chart(fig_edu, use_container_width=True)

# ═══════════════════════════════════════════════════════
# Raw Data Table
# ═══════════════════════════════════════════════════════
st.subheader("📋 Raw Data")

# Format numeric columns
format_dict = {}
for col in filtered_df.select_dtypes(include=["number"]).columns:
    if "salary" in col or "final" in col:
        format_dict[col] = "${:,.0f}"
    elif "bonus" in col or "percentage" in col:
        format_dict[col] = "{:.1f}%"
    else:
        format_dict[col] = "{:.1f}"

st.dataframe(
    filtered_df.style.format(format_dict),
    use_container_width=True,
    height=400,
)

# ── Footer ──
st.markdown("---")
st.caption(
    "Data source: [Kaggle Employee Salary Dataset]"
    "(https://www.kaggle.com/datasets/prince7489/employee-salary-dataset) "
    "| ETL: Apache Airflow | DB: MySQL | Dashboard: Streamlit"
)
