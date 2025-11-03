import streamlit as st
from pymongo import MongoClient
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import datetime
from datetime import timedelta

st.set_page_config(
    layout="wide", 
    page_title="Crypto Intelligence Dashboard",
    initial_sidebar_state="expanded",
    page_icon="📈"
)

# Custom CSS for modern styling
st.markdown("""
<style>
    .main-header {
        background: linear-gradient(90deg, #667eea 0%, #764ba2 100%);
        padding: 2rem;
        border-radius: 10px;
        color: white;
        text-align: center;
        margin-bottom: 2rem;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
    }
    
    .metric-card {
        background: white;
        padding: 1.5rem;
        border-radius: 10px;
        box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
        border-left: 4px solid #667eea;
        margin-bottom: 1rem;
    }
    
    .metric-value {
        font-size: 2rem;
        font-weight: bold;
        color: #2c3e50;
    }
    
    .metric-label {
        font-size: 0.9rem;
        color: #7f8c8d;
        text-transform: uppercase;
        letter-spacing: 0.5px;
    }
    
    .positive-change {
        color: #27ae60;
    }
    
    .negative-change {
        color: #e74c3c;
    }
    
    .confidence-high {
        background: linear-gradient(45deg, #27ae60, #2ecc71);
        color: white;
        padding: 0.5rem 1rem;
        border-radius: 20px;
        font-weight: bold;
    }
    
    .confidence-medium {
        background: linear-gradient(45deg, #f39c12, #e67e22);
        color: white;
        padding: 0.5rem 1rem;
        border-radius: 20px;
        font-weight: bold;
    }
    
    .confidence-low {
        background: linear-gradient(45deg, #e74c3c, #c0392b);
        color: white;
        padding: 0.5rem 1rem;
        border-radius: 20px;
        font-weight: bold;
    }
    
    .sidebar .sidebar-content {
        background: linear-gradient(180deg, #667eea 0%, #764ba2 100%);
    }
    
    .stTabs [data-baseweb="tab-list"] {
        gap: 2px;
    }
    
    .stTabs [data-baseweb="tab"] {
        height: 50px;
        padding-left: 20px;
        padding-right: 20px;
        background: #f8f9fa;
        border-radius: 5px 5px 0 0;
    }
    
    .stTabs [aria-selected="true"] {
        background: #667eea;
        color: white;
    }
    
    .stTabs [data-baseweb="tab"] {
        color: #2c3e50 !important;
    }
    
    .stTabs [data-baseweb="tab"]:hover {
        color: #e74c3c !important;
    }
</style>
""", unsafe_allow_html=True)

# Add cache control headers
st.markdown("""
<meta http-equiv="Cache-Control" content="no-cache, no-store, must-revalidate">
<meta http-equiv="Pragma" content="no-cache">
<meta http-equiv="Expires" content="0">
""", unsafe_allow_html=True)

# Main header
st.markdown("""
<div class="main-header">
    <h1> Crypto Intelligence Dashboard</h1>
    <p>Advanced ML Predictions & Market Analytics</p>
</div>
""", unsafe_allow_html=True)

# Sidebar controls
st.sidebar.markdown("## 🎛️ Dashboard Controls")
st.sidebar.markdown("---")

# Refresh controls
if st.sidebar.button("Refresh Data", type="primary", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

# Filter options
st.sidebar.markdown("### Filter Options")
confidence_filter = st.sidebar.selectbox(
    "Prediction Confidence",
    ["All", "High", "Medium", "Low"],
    index=0
)

volatility_filter = st.sidebar.selectbox(
    "Volatility Level",
    ["All", "Low (< 5%)", "Medium (5-15%)", "High (> 15%)"],
    index=0
)

# Data source info, adjusted timezone GMT +7
st.sidebar.markdown("---")
st.sidebar.markdown("### Data Sources")
st.sidebar.markdown("• **MongoDB**: Batch predictions")
st.sidebar.markdown("• **HDFS**: Historical data")
st.sidebar.markdown("• **Yahoo Finance**: Market data")
st.sidebar.markdown(f"• **Last updated**: {pd.Timestamp.now('Asia/Bangkok').strftime('%H:%M:%S')}")

# MongoDB connection with error handling
@st.cache_data(ttl=10)  # Cache for only 10 seconds
def get_mongodb_data():
    try:
        client = MongoClient("mongodb://mongodb:27017", serverSelectionTimeoutMS=5000)
        collection = client["crypto_batch"]["predictions"]
        data = list(collection.find())
        client.close()
        return data
    except Exception as e:
        st.error(f"Failed to connect to MongoDB: {e}")
        return []

# Get data
with st.spinner("🔄 Loading prediction data..."):
    data = get_mongodb_data()

if not data:
    st.error("❌ No data found in MongoDB. Please ensure the batch processing pipeline is running.")
    st.stop()

df = pd.DataFrame(data)
df["symbol"] = df["symbol"].str.upper()

# Apply filters
if confidence_filter != "All":
    df = df[df["prediction_confidence"] == confidence_filter.lower()]

if volatility_filter != "All":
    if volatility_filter == "Low (< 5%)":
        df = df[df["volatility_ratio"] < 0.05]
    elif volatility_filter == "Medium (5-15%)":
        df = df[(df["volatility_ratio"] >= 0.05) & (df["volatility_ratio"] <= 0.15)]
    elif volatility_filter == "High (> 15%)":
        df = df[df["volatility_ratio"] > 0.15]

# Outlier clipping for better visualization
# clip_cols = ["volume", "volatility_7d", "volatility_14d", "volatility_30d"]
# for c in clip_cols:
#     if c in df.columns:
#         df[c] = np.clip(df[c], 0, np.percentile(df[c], 95))

# Debug: Print available columns
# st.sidebar.markdown("---")
# st.sidebar.markdown("### 🔍 Debug Info")
# st.sidebar.write(f"**Available columns:** {len(df.columns)}")
# st.sidebar.write(f"**Data shape:** {df.shape}")
# if st.sidebar.checkbox("Show column names"):
#     st.sidebar.write(list(df.columns))

# Calculate key metrics
total_market_cap = (df['predicted_price'] * df['volume']).sum()
best_performer = df.loc[df['prediction_change_pct'].idxmax()]
worst_performer = df.loc[df['prediction_change_pct'].idxmin()]

# Convert confidence strings to numeric scores for calculations
confidence_mapping = {'high': 0.8, 'medium': 0.5, 'low': 0.2}
df['confidence_score'] = df['prediction_confidence'].map(confidence_mapping)
avg_confidence = df['confidence_score'].mean()
high_confidence_count = len(df[df['prediction_confidence'] == 'high'])

# Calculate missing MA ratios from existing MA columns
df['ma_7_14_ratio'] = df['ma_7d'] / df['ma_14d']
df['ma_14_30_ratio'] = df['ma_14d'] / df['ma_30d']
df['ma_30_90_ratio'] = df['ma_30d'] / df['ma_90d']

# Key Performance Indicators
st.markdown("## Key Performance Indicators")
kpi1, kpi2, kpi3, kpi4 = st.columns(4)

with kpi1:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-value">${total_market_cap:,.0f}</div>
        <div class="metric-label">Total Market Cap</div>
    </div>
    """, unsafe_allow_html=True)

with kpi2:
    change_color = "positive-change" if best_performer['prediction_change_pct'] > 0 else "negative-change"
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-value {change_color}">{best_performer['prediction_change_pct']:.2f}%</div>
        <div class="metric-label">Best Performer ({best_performer['symbol']})</div>
    </div>
    """, unsafe_allow_html=True)

with kpi3:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-value">{avg_confidence:.2f}</div>
        <div class="metric-label">Avg Confidence Score</div>
    </div>
    """, unsafe_allow_html=True)

with kpi4:
    st.markdown(f"""
    <div class="metric-card">
        <div class="metric-value">{high_confidence_count}/{len(df)}</div>
        <div class="metric-label">High Confidence Predictions</div>
    </div>
    """, unsafe_allow_html=True)

# Main dashboard tabs
st.markdown("---")
tabs = st.tabs(["🎯 Predictions Overview", "📈 Price Analysis", "🔍 Risk Assessment", "🤖 Model Performance", "💼 Portfolio Insights", "📊 Raw Data"])

with tabs[0]:
    st.markdown("### Prediction Overview")
    
    # Prediction confidence distribution
    col1, col2 = st.columns(2)
    
    with col1:
        # Confidence distribution
        conf_counts = df['prediction_confidence'].value_counts()
        fig_conf = px.pie(values=conf_counts.values, names=conf_counts.index, 
                        title="Prediction Confidence Distribution",
                        color_discrete_map={'high': '#27ae60', 'medium': '#f39c12', 'low': '#e74c3c'})
        fig_conf.update_traces(textposition='inside', textinfo='percent+label')
        st.plotly_chart(fig_conf, use_container_width=True)
    
    with col2:
        # Price predictions vs current prices
        # Ensure confidence_score is positive for size
        df_pred_clean = df.copy()
        df_pred_clean['confidence_score_abs'] = np.abs(df_pred_clean['confidence_score'])
        df_pred_clean['confidence_score_abs'] = np.maximum(df_pred_clean['confidence_score_abs'], 0.1)  # Minimum size
        
        fig_pred = px.scatter(df_pred_clean, x='last_price', y='predicted_price', 
                           color='prediction_confidence',
                           size='confidence_score_abs',
                           hover_data=['symbol', 'prediction_change_pct', 'volatility_ratio'],
                           title="Predicted vs Current Prices",
                           color_discrete_map={'high': '#27ae60', 'medium': '#f39c12', 'low': '#e74c3c'})
        
        # Add diagonal line for perfect prediction
        fig_pred.add_trace(go.Scatter(x=[df['last_price'].min(), df['last_price'].max()],
                                    y=[df['last_price'].min(), df['last_price'].max()],
                                    mode='lines', name='Perfect Prediction',
                                    line=dict(dash='dash', color='gray')))
        
        st.plotly_chart(fig_pred, use_container_width=True)
    
    # Top predictions table
    st.markdown("### Top Predictions")
    top_predictions = df.nlargest(5, 'prediction_change_pct')[['symbol', 'last_price', 'predicted_price', 
                                                              'prediction_change_pct', 'prediction_confidence', 
                                                              'confidence_score']].copy()
    top_predictions['prediction_change_pct'] = top_predictions['prediction_change_pct'].round(2)
    top_predictions['confidence_score'] = top_predictions['confidence_score'].round(3)
    st.dataframe(top_predictions, use_container_width=True)

with tabs[1]:
    st.markdown("### Price Analysis")
    
    # Price comparison chart
    fig_price = go.Figure()
    
    # Add current prices
    fig_price.add_trace(go.Bar(
        name='Current Price',
        x=df['symbol'],
        y=df['last_price'],
        marker_color='lightblue'
    ))
    
    # Add predicted prices
    fig_price.add_trace(go.Bar(
        name='Predicted Price',
        x=df['symbol'],
        y=df['predicted_price'],
        marker_color='darkblue'
    ))
    
    fig_price.update_layout(
        title="Current vs Predicted Prices",
        xaxis_title="Cryptocurrency",
        yaxis_title="Price (USD)",
        barmode='group',
        height=500
    )
    
    st.plotly_chart(fig_price, use_container_width=True)
    
    # Price change analysis
    col1, col2 = st.columns(2)
    
    with col1:
        # Price change distribution
        fig_change = px.histogram(df, x='prediction_change_pct', nbins=20,
                                title="Price Change Distribution",
                                color_discrete_sequence=['#667eea'])
        fig_change.add_vline(x=0, line_dash="dash", line_color="red", 
                           annotation_text="No Change")
        st.plotly_chart(fig_change, use_container_width=True)
    
    with col2:
        # Volatility vs price change
        # Ensure volume is positive for size
        df_vol_clean = df.copy()
        df_vol_clean['volume_abs'] = np.abs(df_vol_clean['volume'])
        df_vol_clean['volume_abs'] = np.maximum(df_vol_clean['volume_abs'], 1000)  # Minimum size
        
        fig_vol = px.scatter(df_vol_clean, x='volatility_ratio', y='prediction_change_pct',
                           color='prediction_confidence', size='volume_abs',
                           hover_data=['symbol', 'confidence_score'],
                           title="Volatility vs Price Change",
                           color_discrete_map={'high': '#27ae60', 'medium': '#f39c12', 'low': '#e74c3c'})
        st.plotly_chart(fig_vol, use_container_width=True)

with tabs[2]:
    st.markdown("### Risk Assessment")
    
    # Risk metrics
    risk_col1, risk_col2, risk_col3 = st.columns(3)
    
    with risk_col1:
        avg_volatility = df['volatility_ratio'].mean()
        st.metric("Average Volatility", f"{avg_volatility:.2%}")
    
    with risk_col2:
        high_vol_count = len(df[df['volatility_ratio'] > 0.15])
        st.metric("High Volatility Assets", f"{high_vol_count}/{len(df)}")
    
    with risk_col3:
        risk_score = (df['volatility_ratio'] * (1 - df['confidence_score'])).mean()
        st.metric("Overall Risk Score", f"{risk_score:.3f}")
    
    # Risk heatmap
    risk_df = df[['symbol', 'volatility_ratio', 'confidence_score', 'prediction_change_pct']].copy()
    risk_df['risk_score'] = risk_df['volatility_ratio'] * (1 - risk_df['confidence_score'])
    
    # Ensure risk_score is positive for size
    risk_df['risk_score_abs'] = np.abs(risk_df['risk_score'])
    risk_df['risk_score_abs'] = np.maximum(risk_df['risk_score_abs'], 0.01)  # Minimum size
    
    fig_risk = px.scatter(risk_df, x='volatility_ratio', y='confidence_score',
                         size='risk_score_abs', color='prediction_change_pct',
                         hover_data=['symbol', 'risk_score'],
                         title="Risk Assessment Matrix",
                         color_continuous_scale='RdYlGn')
    
    # Add risk zones
    fig_risk.add_hline(y=0.7, line_dash="dash", line_color="orange", 
                     annotation_text="High Confidence Threshold")
    fig_risk.add_vline(x=0.15, line_dash="dash", line_color="red",
                     annotation_text="High Volatility Threshold")
    
    st.plotly_chart(fig_risk, use_container_width=True)
    
    # Correlation matrix
    st.markdown("#### Feature Correlation Matrix")
    numeric_cols = df.select_dtypes(include=[np.number]).columns
    
    if len(numeric_cols) > 1:
        corr_matrix = df[numeric_cols].corr()
        
        fig_corr = px.imshow(corr_matrix, 
                            text_auto=True, 
                            color_continuous_scale='RdBu',
                            title="Feature Correlation Heatmap",
                            aspect="auto")
        st.plotly_chart(fig_corr, use_container_width=True)
    else:
        st.warning("Not enough numeric columns for correlation analysis")

with tabs[3]:
    st.markdown("### Model Performance")
    
    # Model metrics
    perf_col1, perf_col2, perf_col3 = st.columns(3)
    
    with perf_col1:
        avg_rmse = df['rmse'].mean()
        st.metric("Average RMSE", f"{avg_rmse:.4f}")
    
    with perf_col2:
        avg_mape = df['mape'].mean()
        st.metric("Average MAPE", f"{avg_mape:.2f}%")
    
    with perf_col3:
        avg_mae = df['mae'].mean()
        st.metric("Average MAE", f"{avg_mae:.4f}")
    
    # Model performance comparison
    fig_perf = go.Figure()
    
    fig_perf.add_trace(go.Bar(name='RMSE', x=df['symbol'], y=df['rmse'], marker_color='lightcoral'))
    fig_perf.add_trace(go.Bar(name='MAE', x=df['symbol'], y=df['mae'], marker_color='lightblue'))
    fig_perf.add_trace(go.Bar(name='MAPE', x=df['symbol'], y=df['mape'], marker_color='lightgreen'))
    
    fig_perf.update_layout(
        title="Model Performance Metrics by Cryptocurrency",
        xaxis_title="Cryptocurrency",
        yaxis_title="Error Value",
        barmode='group',
        height=500
    )
    
    st.plotly_chart(fig_perf, use_container_width=True)
    
    # Performance vs confidence
    # Ensure mape is positive for size
    df_perf_clean = df.copy()
    df_perf_clean['mape_abs'] = np.abs(df_perf_clean['mape'])
    df_perf_clean['mape_abs'] = np.maximum(df_perf_clean['mape_abs'], 0.1)  # Minimum size
    
    fig_perf_conf = px.scatter(df_perf_clean, x='confidence_score', y='rmse',
                              color='symbol', size='mape_abs',
                              hover_data=['symbol', 'mae', 'mape'],
                              title="Model Confidence vs Performance")
    st.plotly_chart(fig_perf_conf, use_container_width=True)

with tabs[4]:
    st.markdown("### Portfolio Insights")
    
    # Portfolio allocation recommendations
    st.markdown("#### Recommended Portfolio Allocation")
    
    # Calculate portfolio weights based on confidence and expected returns
    df_portfolio = df.copy()
    df_portfolio['expected_return'] = df_portfolio['prediction_change_pct'] / 100
    df_portfolio['risk_adjusted_return'] = df_portfolio['expected_return'] / (df_portfolio['volatility_ratio'] + 0.01)
    df_portfolio['confidence_weight'] = df_portfolio['confidence_score'] ** 2
    df_portfolio['portfolio_weight'] = (df_portfolio['risk_adjusted_return'] * df_portfolio['confidence_weight'])
    df_portfolio['portfolio_weight'] = df_portfolio['portfolio_weight'] / df_portfolio['portfolio_weight'].sum()
    
    # Top 5 recommendations
    top_portfolio = df_portfolio.nlargest(5, 'portfolio_weight')[['symbol', 'portfolio_weight', 'expected_return', 'volatility_ratio', 'confidence_score']]
    top_portfolio['portfolio_weight'] = (top_portfolio['portfolio_weight'] * 100).round(2)
    top_portfolio['expected_return'] = (top_portfolio['expected_return'] * 100).round(2)
    top_portfolio['volatility_ratio'] = (top_portfolio['volatility_ratio'] * 100).round(2)
    
    st.dataframe(top_portfolio, use_container_width=True)
    
    # Portfolio allocation pie chart
    fig_portfolio = px.pie(top_portfolio, values='portfolio_weight', names='symbol',
                          title="Recommended Portfolio Allocation (Top 5)",
                          color_discrete_sequence=px.colors.qualitative.Set3)
    st.plotly_chart(fig_portfolio, use_container_width=True)
    
    # Risk-return scatter
    # Handle negative values for size by using absolute values and adding a minimum size
    df_portfolio_clean = df_portfolio.copy()
    df_portfolio_clean['portfolio_weight_abs'] = np.abs(df_portfolio_clean['portfolio_weight'])
    df_portfolio_clean['portfolio_weight_abs'] = np.maximum(df_portfolio_clean['portfolio_weight_abs'], 0.01)  # Minimum size
    
    fig_risk_return = px.scatter(df_portfolio_clean, x='volatility_ratio', y='expected_return',
                               color='confidence_score', size='portfolio_weight_abs',
                               hover_data=['symbol', 'portfolio_weight'],
                               title="Risk-Return Analysis",
                               color_continuous_scale='Viridis')
    st.plotly_chart(fig_risk_return, use_container_width=True)
    
    # Trend Analysis Section
    st.markdown("#### Trend Analysis & Momentum Indicators")
    
    # Calculate trend indicators
    df_trend = df.copy()
    df_trend['momentum_score'] = (df_trend['momentum_5d'] + df_trend['momentum_10d'] + df_trend['momentum_20d']) / 3
    df_trend['trend_strength'] = abs(df_trend['momentum_score'])
    df_trend['trend_direction'] = ['Bullish' if x > 0 else 'Bearish' for x in df_trend['momentum_score']]
    
    # Trend strength visualization
    fig_trend = px.bar(df_trend, x='symbol', y='trend_strength', 
                      color='trend_direction',
                      title="Trend Strength by Cryptocurrency",
                      color_discrete_map={'Bullish': '#27ae60', 'Bearish': '#e74c3c'})
    st.plotly_chart(fig_trend, use_container_width=True)
    
    # Moving averages analysis
    st.markdown("#### Moving Averages Analysis")
    
    # Create subplot for moving averages
    fig_ma = make_subplots(rows=2, cols=1, 
                          subplot_titles=('Price vs Moving Averages', 'MA Ratios Analysis'),
                          vertical_spacing=0.1)
    
    # Price vs MAs
    for symbol in df['symbol'].unique():
        symbol_data = df[df['symbol'] == symbol].iloc[0]
        fig_ma.add_trace(go.Scatter(
            x=[symbol], y=[symbol_data['last_price']],
            mode='markers', name=f'{symbol} Price',
            marker=dict(size=10, color='blue')
        ))
        fig_ma.add_trace(go.Scatter(
            x=[symbol], y=[symbol_data['ma_7d']],
            mode='markers', name=f'{symbol} MA7',
            marker=dict(size=8, color='orange')
        ))
        fig_ma.add_trace(go.Scatter(
            x=[symbol], y=[symbol_data['ma_14d']],
            mode='markers', name=f'{symbol} MA14',
            marker=dict(size=8, color='green')
        ))
    
    # MA Ratios
    ma_ratios = df[['symbol', 'ma_7_14_ratio', 'ma_14_30_ratio', 'ma_30_90_ratio']].copy()
    ma_ratios_melted = ma_ratios.melt(id_vars=['symbol'], var_name='MA_Ratio', value_name='Ratio')
    
    fig_ma2 = px.bar(ma_ratios_melted, x='symbol', y='Ratio', color='MA_Ratio',
                    title="Moving Average Ratios",
                    barmode='group')
    
    col1, col2 = st.columns(2)
    with col1:
        st.plotly_chart(fig_ma, use_container_width=True)
    with col2:
        st.plotly_chart(fig_ma2, use_container_width=True)

with tabs[5]:
    st.markdown("### Raw Data & System Health")
    
    # System Health Monitoring
    st.markdown("#### System Health Dashboard")
    
    health_col1, health_col2, health_col3, health_col4 = st.columns(4)
    
    with health_col1:
        # Data freshness
        data_age = (pd.Timestamp.now() - pd.Timestamp.now()).total_seconds() / 3600  # Hours
        health_status = "🟢 Healthy" if data_age < 24 else "🟡 Stale" if data_age < 48 else "🔴 Critical"
        st.metric("Data Freshness", health_status)
    
    with health_col2:
        # Model coverage
        coverage = len(df) / 10 * 100  # Assuming 10 expected cryptocurrencies
        st.metric("Model Coverage", f"{coverage:.1f}%")
    
    with health_col3:
        # Data quality score
        quality_score = df['confidence_score'].mean() * 100
        st.metric("Data Quality", f"{quality_score:.1f}%")
    
    with health_col4:
        # System status
        system_status = "🟢 Online" if len(df) > 0 else "🔴 Offline"
        st.metric("System Status", system_status)
    
    # Data Quality Metrics
    st.markdown("#### Data Quality Metrics")
    
    quality_col1, quality_col2 = st.columns(2)
    
    with quality_col1:
        # Missing data analysis
        missing_data = df.isnull().sum()
        if missing_data.sum() > 0:
            fig_missing = px.bar(x=missing_data.index, y=missing_data.values,
                               title="Missing Data by Column",
                               color=missing_data.values,
                               color_continuous_scale='Reds')
            st.plotly_chart(fig_missing, use_container_width=True)
        else:
            st.success("✅ No missing data detected")
    
    with quality_col2:
        # Data distribution
        fig_dist = px.histogram(df, x='confidence_score', nbins=20,
                               title="Confidence Score Distribution",
                               color_discrete_sequence=['#667eea'])
        st.plotly_chart(fig_dist, use_container_width=True)
    
    # Data summary
    st.markdown("#### Data Summary")
    st.write(f"**Total Records:** {len(df)}")
    st.write(f"**Available Cryptocurrencies:** {', '.join(df['symbol'].unique())}")
    st.write(f"**Data Points per Symbol:** {len(df) // len(df['symbol'].unique()) if len(df) > 0 else 0}")
    st.write(f"**Average Confidence Score:** {df['confidence_score'].mean():.3f}")
    st.write(f"**High Confidence Predictions:** {len(df[df['prediction_confidence'] == 'high'])}")
    
    # Technical indicators summary
    st.markdown("#### Technical Indicators Summary")
    
    tech_indicators = ['rsi', 'macd', 'bb_position', 'bb_width', 'momentum_5d', 'momentum_10d', 'momentum_20d']
    # Filter to only include columns that exist in the dataframe
    available_tech_indicators = [col for col in tech_indicators if col in df.columns]
    
    if available_tech_indicators:
        tech_summary = df[available_tech_indicators].describe()
        st.dataframe(tech_summary, use_container_width=True)
    else:
        st.warning("No technical indicators available in the data")
    
    # Full dataset
    st.markdown("#### Complete Dataset")
    
    # Add search and filter options
    search_symbol = st.text_input("🔍 Search by Symbol:", placeholder="e.g., BTC, ETH")
    if search_symbol:
        df_filtered = df[df['symbol'].str.contains(search_symbol.upper(), case=False)]
    else:
        df_filtered = df
    
    st.dataframe(df_filtered, use_container_width=True)
    
    # Download options
    st.markdown("#### Data Export")
    col1, col2 = st.columns(2)
    
    with col1:
        csv = df_filtered.to_csv(index=False)
        st.download_button(
            label="📥 Download Filtered Data as CSV",
            data=csv,
            file_name=f"crypto_predictions_filtered_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv"
        )
    
    with col2:
        # Export summary statistics
        summary_stats = df.describe()
        csv_summary = summary_stats.to_csv()
        st.download_button(
            label="📊 Download Summary Statistics",
            data=csv_summary,
            file_name=f"crypto_summary_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv"
        )
    
    # System recommendations
    st.markdown("#### System Recommendations")
    
    recommendations = []
    
    if df['confidence_score'].mean() < 0.6:
        recommendations.append("⚠️ Low average confidence score - consider model retraining")
    
    if len(df[df['prediction_confidence'] == 'low']) > len(df) * 0.5:
        recommendations.append("⚠️ High number of low-confidence predictions - review feature engineering")
    
    if df['volatility_ratio'].mean() > 0.2:
        recommendations.append("⚠️ High market volatility detected - consider risk management")
    
    if len(df) < 5:
        recommendations.append("⚠️ Limited data points - ensure all models are running")
    
    if not recommendations:
        st.success("✅ All systems operating normally")
    else:
        for rec in recommendations:
            st.warning(rec)