# file: dash_app.py
import os
import dash
from dash import dcc, html
from dash.dependencies import Input, Output
import requests
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

# --- Khởi tạo Dash ---
app = dash.Dash(__name__)
app.title = "VCI Dashboard"

# --- Layout ---
app.layout = html.Div([
    html.H1("VCI Stock Dashboard", style={'textAlign': 'center', 'marginBottom': '30px'}),

    # Prediction info
    html.Div(id='prediction-div', style={
        'textAlign':'center', 
        'margin':'20px', 
        'fontSize':'20px',
        'padding': '15px',
        'backgroundColor': '#f0f0f0',
        'borderRadius': '10px',
        'fontWeight': 'bold'
    }),

    # Dropdown chọn resample
    html.Div([
        html.Label("Chọn quãng thời gian:", style={'fontWeight': 'bold', 'marginRight': '10px'}),
        dcc.Dropdown(
            id='period-dropdown',
            options=[
                {'label': 'Ngày', 'value': 'D'},
                {'label': 'Tuần', 'value': 'W'},
                {'label': 'Tháng', 'value': 'M'}
            ],
            value='D',
            clearable=False,
            style={'width': '200px'}
        )
    ], style={'width':'400px', 'margin':'auto', 'marginBottom': '30px', 'display': 'flex', 'alignItems': 'center'}),

    # Charts grid
    html.Div([
        html.Div([dcc.Graph(id='candlestick-chart')], style={'width':'100%', 'marginBottom':'20px'}),

        html.Div([
            html.Div([dcc.Graph(id='line-chart')], style={'width':'48%', 'display':'inline-block'}),
            html.Div([dcc.Graph(id='volume-chart')], style={'width':'48%', 'display':'inline-block', 'marginLeft':'2%'}),
        ]),

        html.Div([
            html.Div([dcc.Graph(id='seasonal-chart')], style={'width':'48%', 'display':'inline-block'}),
            html.Div([dcc.Graph(id='trend-chart')], style={'width':'48%', 'display':'inline-block', 'marginLeft':'2%'}),
        ]),

        html.Div([dcc.Graph(id='corr-chart')], style={'width':'100%'})
    ])
])

# --- Callback ---
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
    # --- Lấy dữ liệu history ---
    URL_HISTORY = os.environ.get('DATABASE_HOST', 'http://database_service:7001') + f"/history?period={period}"
    try:
        df = pd.DataFrame(requests.get(URL_HISTORY).json())
        df['time'] = pd.to_datetime(df['time'])
        df.set_index('time', inplace=True)
    except:
        df = pd.DataFrame(columns=['open','high','low','close','volume'])

    # --- Lấy dữ liệu clustering ---
    URL_CLUSTER = os.environ.get('CLUSTERING_HOST', 'http://clustering_service:7003') + "/clustering"
    try:
        df_cluster = pd.DataFrame(requests.get(URL_CLUSTER).json())
        df_cluster['time'] = pd.to_datetime(df_cluster['time'])
        df_merged = pd.merge_asof(
            df.sort_index(),
            df_cluster[['time','cluster_label','trend_label']].sort_values('time'),
            on='time',
            direction='backward'
        )
        df_merged.set_index('time', inplace=True)
        df_merged['cluster_label'] = df_merged['cluster_label'].fillna('Unknown')
        df_merged['trend_label'] = df_merged['trend_label'].fillna('Unknown')
    except:
        df_merged = df.copy()
        df_merged['cluster_label'] = 'Unknown'
        df_merged['trend_label'] = 'Unknown'

    # --- Prediction ---
    URL_PRED = os.environ.get('PREDICTION_HOST', 'http://prediction_service:7002') + "/prediction"
    try:
        pred = requests.get(URL_PRED).json()
        pred_text = html.Div([
            html.Span("🔮 Dự đoán ngày mai: "),
            html.Span(f"{pred.get('pred_class','?')}", style={
                'color':'#28a745' if pred.get('pred_class')=='Tăng' else '#dc3545',
                'fontWeight':'bold','fontSize':'24px'
            }),
            html.Span(f" | Giá hôm nay: {pred.get('current_price','?')} | Giá dự báo ngày mai: {pred.get('pred_price','?')}", style={'fontSize':'18px'})
        ])
    except:
        pred_text = "Không lấy được dữ liệu prediction"

    # --- CANDLESTICK chart ---
    color_map_trend = {'Tăng':'#28a745','Giảm':'#dc3545'}
    fig_candlestick = go.Figure()
    fig_candlestick.add_trace(go.Candlestick(
        x=df_merged.index,
        open=df_merged['open'],
        high=df_merged['high'],
        low=df_merged['low'],
        close=df_merged['close'],
        name='OHLC',
        increasing_line_color='#26a69a',
        decreasing_line_color='#ef5350'
    ))
    fig_candlestick.update_layout(title='Biểu đồ Nến', xaxis_rangeslider_visible=False, height=500, hovermode='x unified')

    # --- LINE chart ---
    fig_line = px.line(df.reset_index(), x='time', y='close', title='Giá Đóng Cửa', markers=True)

    # --- VOLUME chart ---
    fig_volume = px.bar(df.reset_index(), x='time', y='volume', title='Volume')

    # --- SEASONAL chart ---
    df['month'] = df.index.month
    seasonal = df.groupby('month')['close'].mean().reset_index()
    fig_seasonal = px.line(seasonal, x='month', y='close', title='Giá theo Tháng', markers=True)

    # --- TREND chart ---
    df_trend = df['close'].resample('M').mean().reset_index()
    df_trend.columns = ['time','close']
    fig_trend = px.line(df_trend, x='time', y='close', title='Xu hướng Giá', markers=True)

    # --- CORRELATION chart ---
    corr = df[['open','high','low','close','volume']].corr()
    fig_corr = px.imshow(corr, text_auto='.2f', title='Correlation', color_continuous_scale='RdBu_r')

    return fig_candlestick, fig_line, fig_volume, fig_seasonal, fig_trend, fig_corr, pred_text

# --- Run server ---
if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=8000)
