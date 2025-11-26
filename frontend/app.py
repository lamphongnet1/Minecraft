import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import requests
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import os # <--- ĐÃ SỬA: Thêm thư viện os

# --- Khai báo các URL API bằng biến môi trường ---
# Dùng os.environ.get() để đọc các URL đã định nghĩa trong docker-compose.yml
# Fallback về localhost:port nếu không chạy trong Docker
URL_HISTORY = os.environ.get('DATABASE_URL', 'http://127.0.0.1:7001') + "/history"
URL_CLUSTER = os.environ.get('CLUSTERING_URL', 'http://127.0.0.1:7003') + "/clustering"
URL_PRED = os.environ.get('PREDICTION_URL', 'http://127.0.0.1:7002') + "/prediction"


# --- Khởi tạo Dash ---
app = dash.Dash(__name__)
app.title = "VCI Dashboard"

# --- Layout ---
app.layout = html.Div([
    html.H1("VCI Stock Dashboard", style={'textAlign': 'center', 'marginBottom': '30px', 'color': '#2C3E50'}),
    
    # Prediction info
    html.Div(id='prediction-div', style={
        'textAlign':'center', 
        'margin':'20px auto', 
        'fontSize':'20px',
        'padding': '15px',
        'backgroundColor': '#ECF0F1',
        'borderRadius': '10px',
        'fontWeight': 'bold',
        'maxWidth': '800px'
    }),
    
    # Dropdown chọn resample
    html.Div([
        html.Label("Chọn quãng thời gian:", style={'fontWeight': 'bold', 'marginRight': '10px', 'color': '#34495E'}),
        dcc.Dropdown(
            id='period-dropdown',
            options=[
                {'label': 'Ngày (D)', 'value': 'D'},
                {'label': 'Tuần (W)', 'value': 'W'},
                {'label': 'Tháng (M)', 'value': 'M'}
            ],
            value='D',
            clearable=False,
            style={'width': '200px', 'boxShadow': '2px 2px 5px #ccc'}
        )
    ], style={'width':'400px', 'margin':'auto', 'marginBottom': '30px', 'display': 'flex', 'alignItems': 'center'}),
    
    # Charts grid
    html.Div([
        # 1. Candlestick với Clustering
        html.Div([
            dcc.Graph(id='candlestick-chart', config={'displayModeBar': False}),
        ], style={'width': '100%', 'marginBottom': '20px'}),
        
        # 2. Row 1: Line + Volume
        html.Div([
            html.Div([dcc.Graph(id='line-chart', config={'displayModeBar': False})], style={'width': '48%', 'display': 'inline-block'}),
            html.Div([dcc.Graph(id='volume-chart', config={'displayModeBar': False})], style={'width': '48%', 'display': 'inline-block', 'marginLeft': '4%'}),
        ], className="row"),
        
        # 3. Row 2: Seasonal + Trend
        html.Div([
            html.Div([dcc.Graph(id='seasonal-chart', config={'displayModeBar': False})], style={'width': '48%', 'display': 'inline-block'}),
            html.Div([dcc.Graph(id='trend-chart', config={'displayModeBar': False})], style={'width': '48%', 'display': 'inline-block', 'marginLeft': '4%'}),
        ], className="row", style={'marginTop': '20px'}),
        
        # 4. Correlation
        html.Div([
            dcc.Graph(id='corr-chart', config={'displayModeBar': False})
        ], style={'width': '100%', 'marginTop': '20px'})
        
    ], style={'padding': '0 20px'})
])

# --- Callback cập nhật tất cả chart & prediction ---
@app.callback(
    Output('candlestick-chart', 'figure'),
    Output('line-chart', 'figure'),
    Output('volume-chart', 'figure'),
    Output('seasonal-chart', 'figure'),
    Output('trend-chart', 'figure'),
    Output('corr-chart', 'figure'),
    Output('prediction-div', 'children'),
    Input('period-dropdown', 'value')
)
def update_charts(period):
    df_resampled = pd.DataFrame()
    
    # --- 0. Lấy dữ liệu history ---
    try:
        url_history_resampled = f"{URL_HISTORY}?period={period}"
        response = requests.get(url_history_resampled)
        response.raise_for_status() # Raise exception for bad status codes
        df_resampled = pd.DataFrame(response.json())
        df_resampled['time'] = pd.to_datetime(df_resampled['time'])
        df_resampled.set_index('time', inplace=True)
    except requests.exceptions.ConnectionError:
        error_msg = html.Div(f"Lỗi kết nối: Không thể kết nối tới Database Service tại {URL_HISTORY}. Hãy kiểm tra Docker Compose.", style={'color': '#E74C3C'})
        # Trả về các biểu đồ rỗng nếu lỗi kết nối
        return (go.Figure(), go.Figure(), go.Figure(), go.Figure(), go.Figure(), go.Figure(), error_msg)
    except requests.exceptions.RequestException as e:
        error_msg = html.Div(f"Lỗi truy vấn Database: {e}", style={'color': '#E74C3C'})
        return (go.Figure(), go.Figure(), go.Figure(), go.Figure(), go.Figure(), go.Figure(), error_msg)

    # --- Khởi tạo cho trường hợp lỗi ---
    df_merged = df_resampled.copy()
    df_merged['cluster_label'] = 'Unknown'

    # --- Lấy dữ liệu clustering ---
    try:
        response = requests.get(URL_CLUSTER)
        response.raise_for_status()
        df_cluster_raw = pd.DataFrame(response.json())
        
        df_cluster = pd.DataFrame(df_cluster_raw)
        df_cluster['time'] = pd.to_datetime(df_cluster['time'])
        # ĐÃ SỬA: Đổi tên cột 'cluster' (từ backend) thành 'cluster_label' cho plotting
        df_cluster = df_cluster.rename(columns={'cluster': 'cluster_label'})
        
        # --- Merge history (resampled) với cluster_label ---
        # df_cluster chỉ có dữ liệu daily, merge_asof gán nhãn cụm gần nhất
        df_merged = pd.merge_asof(
            df_resampled.sort_index().reset_index(), 
            df_cluster[['time', 'cluster_label']].sort_values('time'),
            on='time', 
            direction='backward'
        )
        df_merged.set_index('time', inplace=True)
        
    except requests.exceptions.RequestException as e:
        print(f"Lỗi kết nối/truy vấn Clustering Service: {e}")
        # Nếu lỗi clustering, tiếp tục với biểu đồ không có nhãn cụm
        
    # --- Lấy dữ liệu prediction ---
    try:
        response = requests.get(URL_PRED)
        response.raise_for_status()
        pred = response.json()
        
        pred_text = html.Div([
            html.Span("🔮 Dự đoán ngày mai: ", style={'color': '#34495E'}),
            html.Span(f"{pred['pred_class']}", style={
                'color': '#2ECC71' if pred['pred_class'] == 'Tăng' else ('#E74C3C' if pred['pred_class'] == 'Giảm' else '#F39C12'),
                'fontWeight': 'bold',
                'fontSize': '24px'
            }),
            html.Span(f" | Giá hiện tại: {pred['current_price']:.2f} | Giá dự đoán: {pred['pred_price']:.2f}", style={'fontSize': '18px'}),
        ])
    except requests.exceptions.RequestException as e:
        pred_text = html.Div(f"Lỗi kết nối tới Prediction Service: {e}", style={'color': '#E74C3C'})
        
    
    # ========== 1. CANDLESTICK với CLUSTERING ==========
    fig_candlestick = go.Figure()
    
    # 1.1 Candlestick Trace
    fig_candlestick.add_trace(go.Candlestick(
        x=df_merged.index,
        open=df_merged['open'],
        high=df_merged['high'],
        low=df_merged['low'],
        close=df_merged['close'],
        name='Giá OHLC',
        increasing_line_color='#26a69a',
        decreasing_line_color='#ef5350'
    ))
    
    # 1.2 Scatter Trace cho Clustering
    if 'cluster_label' in df_merged.columns and df_merged['cluster_label'].nunique() > 1:
        fig_candlestick.add_trace(go.Scatter(
            x=df_merged.index,
            y=df_merged['close'],
            mode='markers',
            marker=dict(
                size=8,
                color=df_merged['cluster_label'].astype(str),
                colorscale=px.colors.qualitative.Bold,
                line=dict(width=1, color='Black')
            ),
            name='Nhãn Cụm (K-Means)',
            yaxis='y'
        ))
    
    fig_candlestick.update_layout(
        title=f'Biểu đồ Nến VCI ({period}) với Phân cụm K-Means',
        xaxis_title='Thời gian',
        yaxis_title='Giá',
        xaxis_rangeslider_visible=False,
        height=600,
        hovermode='x unified',
        template='plotly_white'
    )
    
    # ========== 2. LINE CHART - Giá đóng cửa đơn giản ==========
    fig_line = px.line(
        df_resampled.reset_index(),
        x='time',
        y='close',
        title=f'Giá Đóng Cửa VCI ({period})',
        markers=True,
        color_discrete_sequence=['#3498DB']
    )
    fig_line.update_layout(
        xaxis_title='Thời gian',
        yaxis_title='Giá',
        template='plotly_white'
    )
    
    # ========== 3. VOLUME HISTOGRAM ==========
    df_resampled['color'] = df_resampled.apply(
        lambda row: '#26a69a' if row['close'] >= row['open'] else '#ef5350', axis=1
    )
    
    fig_volume = go.Figure(go.Bar(
        x=df_resampled.index,
        y=df_resampled['volume'],
        marker_color=df_resampled['color']
    ))
    
    fig_volume.update_layout(
        title=f'Khối Lượng Giao Dịch VCI ({period})',
        xaxis_title='Thời gian',
        yaxis_title='Volume',
        template='plotly_white'
    )
    
    # ========== 4. SEASONAL LINE - Giá theo tháng ==========
    df_temp = df_resampled.copy()
    df_temp['month'] = df_temp.index.month
    seasonal = df_temp.groupby('month')['close'].mean().reset_index()
    
    fig_seasonal = px.line(
        seasonal,
        x='month',
        y='close',
        title='Giá Đóng Cửa Theo Tháng (Seasonal)',
        markers=True,
        color_discrete_sequence=['#9B59B6']
    )
    fig_seasonal.update_layout(
        xaxis_title='Tháng',
        yaxis_title='Giá Trung Bình',
        xaxis=dict(tickmode='linear', tick0=1, dtick=1),
        template='plotly_white'
    )
    
    # ========== 5. TREND LINE - Giá theo tháng ==========
    df_trend = df_resampled['close'].resample('M').mean().reset_index()
    df_trend.columns = ['time', 'close']
    
    fig_trend = px.line(
        df_trend,
        x='time',
        y='close',
        title='Xu Hướng Giá Đóng Cửa (Trend)',
        markers=True,
        color_discrete_sequence=['#F39C12']
    )
    fig_trend.update_layout(
        xaxis_title='Tháng',
        yaxis_title='Giá Trung Bình',
        template='plotly_white'
    )
    
    # ========== 6. CORRELATION MATRIX ==========
    corr = df_resampled[['open','high','low','close','volume']].corr()
    
    fig_corr = px.imshow(
        corr,
        text_auto='.2f',
        title='Ma Trận Tương Quan',
        color_continuous_scale='RdBu_r',
        aspect='auto',
        template='plotly_white'
    )
    
    return (fig_candlestick, fig_line, fig_volume, fig_seasonal, 
            fig_trend, fig_corr, pred_text)

# --- Run server ---
if __name__ == '__main__':
    # Chạy trên host 0.0.0.0 và cổng 8050
    app.run(debug=True, host='0.0.0.0', port=8050)