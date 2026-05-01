# stock_viewer.py
import streamlit as st
import akshare as ak
import pandas as pd
import sqlite3
from datetime import datetime, timedelta
import plotly.graph_objects as go
import os
import warnings
warnings.filterwarnings('ignore')

st.set_page_config(page_title="A股趋势选股结果", layout="wide")
st.title("📈 每日趋势选股结果（均线多头 + MACD金叉 + RSI强势 + 放量）")

# 数据库文件路径（云端运行时会读取仓库根目录下的 stock_scan.db）
DB_PATH = os.path.join(os.path.dirname(__file__), "stock_scan.db")

def load_results(date=None):
    if not os.path.exists(DB_PATH):
        st.error("数据库文件不存在，请先运行扫描脚本。")
        return pd.DataFrame()
    conn = sqlite3.connect(DB_PATH)
    if date:
        df = pd.read_sql_query(f"SELECT * FROM scan_results WHERE scan_date = '{date}' ORDER BY score DESC", conn)
    else:
        df = pd.read_sql_query("SELECT * FROM scan_results ORDER BY scan_date DESC, score DESC", conn)
    conn.close()
    return df

def get_available_dates():
    if not os.path.exists(DB_PATH):
        return []
    conn = sqlite3.connect(DB_PATH)
    dates = pd.read_sql_query("SELECT DISTINCT scan_date FROM scan_results ORDER BY scan_date DESC", conn)['scan_date'].tolist()
    conn.close()
    return dates

def fetch_stock_history(symbol, lookback_days=120):
    # 先尝试 akshare 默认接口
    df = _try_fetch_with_akshare(symbol, lookback_days)
    if df is not None:
        return df

    # 如果失败，尝试备用接口（新浪源）
    df = _try_fetch_with_sina(symbol, lookback_days)
    if df is not None:
        return df

    # 如果备用接口也失败，尝试 yfinance
    df = _try_fetch_yfinance(symbol, lookback_days)
    if df is not None:
        return df

    return None

def plot_kline(symbol, name):
    df = fetch_stock_history(symbol, 120)
    if df is None:
        st.warning("无法获取K线数据")
        return
    fig = go.Figure()
    fig.add_trace(go.Candlestick(x=df['date'], open=df['open'], high=df['high'],
                                 low=df['low'], close=df['close'], name='K线'))
    df['ma5'] = df['close'].rolling(5).mean()
    df['ma20'] = df['close'].rolling(20).mean()
    df['ma60'] = df['close'].rolling(60).mean()
    fig.add_trace(go.Scatter(x=df['date'], y=df['ma5'], name='MA5', line=dict(color='orange')))
    fig.add_trace(go.Scatter(x=df['date'], y=df['ma20'], name='MA20', line=dict(color='green')))
    fig.add_trace(go.Scatter(x=df['date'], y=df['ma60'], name='MA60', line=dict(color='purple')))
    fig.update_layout(title=f"{symbol} {name}", xaxis_rangeslider_visible=False, height=500)
    st.plotly_chart(fig, use_container_width=True)

# ---------- 侧边栏 ----------
st.sidebar.header("📅 选择查看日期")
dates = get_available_dates()
if dates:
    selected_date = st.sidebar.selectbox("扫描日期", dates, index=0)
else:
    selected_date = None
    st.sidebar.warning("暂无扫描数据")

# 股票搜索
st.sidebar.markdown("---")
st.sidebar.header("🔎 个股快速查询")
search_code = st.sidebar.text_input("输入股票代码 (如 600519)", max_chars=6)
if st.sidebar.button("查询走势") and search_code:
    st.subheader(f"📊 股票 {search_code} 走势图")
    plot_kline(search_code, f"股票{search_code}")

# ---------- 主区域 ----------
if selected_date:
    df_results = load_results(selected_date)
    if df_results.empty:
        st.info(f"{selected_date} 没有符合条件的股票记录")
    else:
        st.success(f"📊 {selected_date} 共筛选出 {len(df_results)} 只股票")
        display_columns = {
            'symbol': '代码',
            'name': '名称',
            'score': '综合评分',
            'close': '收盘价',
            'ma5': 'MA5',
            'ma20': 'MA20',
            'ma60': 'MA60',
            'rsi': 'RSI',
            'cond_met': '条件数'
        }
        df_display = df_results[list(display_columns.keys())].rename(columns=display_columns)
        st.dataframe(df_display, use_container_width=True)

        st.subheader("🔍 查看推荐个股走势")
        code_list = df_results['symbol'].tolist()
        selected_rec = st.selectbox("选择推荐股票代码", code_list)
        if selected_rec:
            name = df_results[df_results['symbol'] == selected_rec]['name'].iloc[0]
            plot_kline(selected_rec, name)
else:
    st.info("请等待扫描器生成数据后刷新。")
def _try_fetch_with_akshare(symbol, lookback_days):
    try:
        df = ak.stock_zh_a_hist(
            symbol=symbol,
            period="daily",
            start_date=(datetime.now() - timedelta(days=lookback_days*2)).strftime("%Y%m%d"),
            end_date=datetime.now().strftime("%Y%m%d"),
            adjust="qfq"
        )
        if df is None or df.empty:
            return None
        df = df.rename(columns={"日期":"date","开盘":"open","最高":"high","最低":"low","收盘":"close"})
        df["date"] = pd.to_datetime(df["date"])
        return df.sort_values("date")
    except:
        return None

def _try_fetch_with_sina(symbol, lookback_days):
    try:
        df = ak.stock_zh_a_daily(
            symbol=f"sh{symbol}" if symbol.startswith('6') else f"sz{symbol}",
            adjust="qfq"
        )
        if df is None or df.empty:
            return None
        df["date"] = pd.to_datetime(df["date"])
        return df.sort_values("date").tail(lookback_days)
    except:
        return None

def _try_fetch_yfinance(symbol, lookback_days):
    try:
        import yfinance as yf
        if symbol.startswith('6'):
            ticker = f"{symbol}.SS"
        else:
            ticker = f"{symbol}.SZ"
        df = yf.download(ticker, period=f"{lookback_days}d")
        if df is None or df.empty:
            return None
        df = df.reset_index()
        df = df.rename(columns={"Date":"date","Open":"open","High":"high","Low":"low","Close":"close"})
        df["date"] = pd.to_datetime(df["date"])
        return df.sort_values("date")
    except:
        return None