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

# ---------- 数据获取函数（多数据源备用）----------
def fetch_stock_history(symbol, lookback_days=120):
    df = _try_fetch_with_akshare(symbol, lookback_days)
    if df is not None:
        return df
    df = _try_fetch_with_sina(symbol, lookback_days)
    if df is not None:
        return df
    df = _try_fetch_yfinance(symbol, lookback_days)
    if df is not None:
        return df
    return None

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

# ---------- 画图 ----------
def plot_kline(symbol, name):
    df = fetch_stock_history(symbol, 120)
    if df is None:
        st.warning("无法获取K线数据，请稍后重试。")
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

# ---------- 生成操作建议 ----------
def generate_advice(ma_ok, macd_ok, rsi_ok, vol_ok):
    """根据技术条件生成简短的操作建议"""
    if ma_ok and macd_ok and rsi_ok and vol_ok:
        return "🔥 强势突破，可重点关注"
    elif ma_ok and macd_ok and vol_ok:
        return "📈 趋势启动，适当参与"
    elif ma_ok and rsi_ok and vol_ok:
        return "🟢 趋势延续，可持有"
    elif macd_ok and rsi_ok and vol_ok:
        return "⚡ 动能反弹，快进快出"
    elif (ma_ok + macd_ok + rsi_ok) >= 2 and vol_ok:
        return "👀 轻度共振，加入观察"
    else:
        return "🔍 暂不符合核心标准，可跟踪"

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
        # 为每只股票生成操作建议（需要条件列，原数据库没有单独存储，但可通过 close, ma5, ma20, ma60, rsi 近似判断）
        # 注意：原数据库未直接存储 c_ma, c_macd, c_rsi, c_vol，但我们可以用现有数据计算近似值
        # 为简化，在查询结果后增加一列建议，这里假定数据库中已有 close, ma5, ma20, ma60, rsi, cond_met
        # 我们根据 cond_met 含义（满足的核心条件数）和基本逻辑生成建议，但为更准确，可加入成交量检查（无存储则忽略）
        # 更好的做法：在扫描时存储 c_ma, c_macd, c_rsi, c_vol 标志，但当前表结构没有。
        # 这里暂时使用 cond_met 和 rsi 范围给出粗略建议，并提示用户这是基于已有数据的简化建议。
        df_display = df_results.copy()
        # 如果数据库中包含 c_ma 等字段则可直接使用，否则通过 close 与均线关系大致判断
        if 'c_ma' not in df_display.columns:
            df_display['c_ma'] = (df_display['ma5'] > df_display['ma20']) & (df_display['ma20'] > df_display['ma60'])
        if 'c_macd' not in df_display.columns:
            df_display['c_macd'] = False  # 无法从现有数据恢复，默认False
        if 'c_rsi' not in df_display.columns:
            df_display['c_rsi'] = (df_display['rsi'] > 50) & (df_display['rsi'] < 75)
        if 'c_vol' not in df_display.columns:
            df_display['c_vol'] = False  # 同样无法恢复

        # 应用建议函数
        df_display['操作建议'] = df_display.apply(
            lambda row: generate_advice(row['c_ma'], row['c_macd'], row['c_rsi'], row['c_vol']),
            axis=1
        )

        # 展示时包含操作建议列（放在名称前面）
        display_columns = {
            'symbol': '代码',
            'name': '名称',
            '操作建议': '操作建议',  # 新增
            'score': '综合评分',
            'close': '收盘价',
            'ma5': 'MA5',
            'ma20': 'MA20',
            'ma60': 'MA60',
            'rsi': 'RSI',
            'cond_met': '条件数'
        }
        df_display = df_display[list(display_columns.keys())].rename(columns=display_columns)
        st.dataframe(df_display, use_container_width=True)

        st.subheader("🔍 查看推荐个股走势")
        code_list = df_results['symbol'].tolist()
        selected_rec = st.selectbox("选择推荐股票代码", code_list)
        if selected_rec:
            name = df_results[df_results['symbol'] == selected_rec]['name'].iloc[0]
            plot_kline(selected_rec, name)
else:
    st.info("请等待扫描器生成数据后刷新。")