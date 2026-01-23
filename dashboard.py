import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import os
import psycopg2

st.set_page_config(
    page_title="Crypto Data Dashboard",
    page_icon="📊",
    layout="wide"
)

@st.cache_resource
def get_connection():
    return psycopg2.connect(os.environ['DATABASE_URL'])

@st.cache_data(ttl=300)
def get_sources():
    conn = get_connection()
    df = pd.read_sql("SELECT DISTINCT source FROM metrics ORDER BY source", conn)
    return df['source'].tolist()

@st.cache_data(ttl=300)
def get_assets(source):
    conn = get_connection()
    df = pd.read_sql(
        "SELECT DISTINCT asset FROM metrics WHERE source = %s ORDER BY asset",
        conn, params=(source,)
    )
    return df['asset'].tolist()

@st.cache_data(ttl=300)
def get_metrics(source):
    conn = get_connection()
    df = pd.read_sql(
        "SELECT DISTINCT metric_name FROM metrics WHERE source = %s ORDER BY metric_name",
        conn, params=(source,)
    )
    return df['metric_name'].tolist()

@st.cache_data(ttl=60)
def get_data(source, assets, metrics, start_date, end_date):
    conn = get_connection()
    
    assets_str = ','.join([f"'{a}'" for a in assets])
    metrics_str = ','.join([f"'{m}'" for m in metrics])
    
    query = f"""
        SELECT pulled_at, asset, metric_name, value, exchange
        FROM metrics 
        WHERE source = %s 
          AND asset IN ({assets_str})
          AND metric_name IN ({metrics_str})
          AND pulled_at >= %s 
          AND pulled_at <= %s
        ORDER BY pulled_at, asset, metric_name
    """
    
    df = pd.read_sql(query, conn, params=(source, start_date, end_date))
    return df

@st.cache_data(ttl=300)
def get_summary_stats():
    conn = get_connection()
    query = """
        SELECT 
            source,
            COUNT(*) as total_records,
            COUNT(DISTINCT asset) as unique_assets,
            COUNT(DISTINCT metric_name) as unique_metrics,
            MIN(pulled_at) as earliest_data,
            MAX(pulled_at) as latest_data
        FROM metrics
        GROUP BY source
        ORDER BY source
    """
    return pd.read_sql(query, conn)

@st.cache_data(ttl=300)
def get_pull_history():
    conn = get_connection()
    query = """
        SELECT source_name, pulled_at, status, records_count
        FROM pulls
        ORDER BY pulled_at DESC
        LIMIT 50
    """
    return pd.read_sql(query, conn)

st.title("📊 Crypto Data Dashboard")
st.markdown("Explore data from Artemis, DefiLlama, and Velo")

tab1, tab2, tab3 = st.tabs(["📈 Data Explorer", "📋 Summary", "🔄 Pull History"])

with tab1:
    col1, col2 = st.columns([1, 3])
    
    with col1:
        st.subheader("Filters")
        
        sources = get_sources()
        selected_source = st.selectbox("Source", sources, index=0 if sources else None)
        
        if selected_source:
            assets = get_assets(selected_source)
            metrics = get_metrics(selected_source)
            
            selected_assets = st.multiselect(
                "Assets", 
                assets, 
                default=assets[:5] if len(assets) >= 5 else assets,
                max_selections=20
            )
            
            selected_metrics = st.multiselect(
                "Metrics", 
                metrics,
                default=metrics[:3] if len(metrics) >= 3 else metrics,
                max_selections=10
            )
            
            col_start, col_end = st.columns(2)
            with col_start:
                start_date = st.date_input(
                    "Start Date",
                    value=datetime.now() - timedelta(days=7)
                )
            with col_end:
                end_date = st.date_input(
                    "End Date",
                    value=datetime.now()
                )
            
            if selected_source == 'velo':
                show_by_exchange = st.checkbox("Split by Exchange", value=False)
            else:
                show_by_exchange = False
    
    with col2:
        if selected_source and selected_assets and selected_metrics:
            with st.spinner("Loading data..."):
                df = get_data(
                    selected_source, 
                    selected_assets, 
                    selected_metrics,
                    start_date,
                    end_date
                )
            
            if len(df) > 0:
                st.subheader(f"Data from {selected_source.upper()}")
                st.caption(f"{len(df):,} records loaded")
                
                for metric in selected_metrics:
                    metric_df = df[df['metric_name'] == metric].copy()
                    
                    if len(metric_df) > 0:
                        st.markdown(f"### {metric}")
                        
                        if show_by_exchange and 'exchange' in metric_df.columns:
                            fig = px.line(
                                metric_df,
                                x='pulled_at',
                                y='value',
                                color='asset',
                                line_dash='exchange',
                                title=f"{metric} Over Time (by Exchange)",
                                labels={'pulled_at': 'Time', 'value': metric}
                            )
                        else:
                            fig = px.line(
                                metric_df,
                                x='pulled_at',
                                y='value',
                                color='asset',
                                title=f"{metric} Over Time",
                                labels={'pulled_at': 'Time', 'value': metric}
                            )
                        
                        fig.update_layout(height=400)
                        st.plotly_chart(fig, use_container_width=True)
                
                with st.expander("View Raw Data"):
                    st.dataframe(df, use_container_width=True)
                    
                    csv = df.to_csv(index=False)
                    st.download_button(
                        "Download CSV",
                        csv,
                        f"{selected_source}_data.csv",
                        "text/csv"
                    )
            else:
                st.info("No data found for the selected filters")
        else:
            st.info("Select assets and metrics to view data")

with tab2:
    st.subheader("Database Summary")
    
    summary = get_summary_stats()
    
    col1, col2, col3 = st.columns(3)
    
    for i, row in summary.iterrows():
        with [col1, col2, col3][i % 3]:
            st.metric(
                label=row['source'].upper(),
                value=f"{row['total_records']:,} records"
            )
            st.caption(f"Assets: {row['unique_assets']} | Metrics: {row['unique_metrics']}")
            st.caption(f"Range: {row['earliest_data'].strftime('%Y-%m-%d') if row['earliest_data'] else 'N/A'} to {row['latest_data'].strftime('%Y-%m-%d %H:%M') if row['latest_data'] else 'N/A'}")
    
    st.markdown("---")
    
    st.subheader("Records by Source")
    fig = px.bar(
        summary,
        x='source',
        y='total_records',
        color='source',
        title="Total Records by Source"
    )
    st.plotly_chart(fig, use_container_width=True)
    
    st.subheader("Full Summary Table")
    st.dataframe(summary, use_container_width=True)

with tab3:
    st.subheader("Recent Pull History")
    
    history = get_pull_history()
    
    success_count = len(history[history['status'] == 'success'])
    total_count = len(history)
    
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Recent Pulls", total_count)
    with col2:
        st.metric("Successful", success_count)
    with col3:
        st.metric("Success Rate", f"{success_count/total_count*100:.1f}%" if total_count > 0 else "N/A")
    
    st.dataframe(
        history.style.apply(
            lambda x: ['background-color: #d4edda' if v == 'success' else 'background-color: #f8d7da' if v in ['no_data', 'error'] else '' for v in x],
            subset=['status']
        ),
        use_container_width=True
    )

st.sidebar.markdown("---")
st.sidebar.caption("Data refreshes every 5 minutes. Use browser refresh for immediate update.")
if st.sidebar.button("🔄 Clear Cache"):
    st.cache_data.clear()
    st.rerun()
