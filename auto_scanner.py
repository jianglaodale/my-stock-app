# auto_scanner.py (云端版，用于 GitHub Actions)
import akshare as ak
import pandas as pd
import numpy as np
from datetime import datetime, timedelta, date
from stockstats import wrap
import time
import warnings
import sqlite3
import os
import sys

warnings.filterwarnings('ignore')

# ---------- 配置 ----------
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "stock_scan.db")
LAST_RUN_FILE = os.path.join(BASE_DIR, "last_run.txt")

# ---------- 交易日判断（使用 akshare 在线获取）----------
def is_trade_day(check_date=None):
    if check_date is None:
        check_date = date.today()
    if check_date.weekday() >= 5:           # 周末
        return False
    try:
        df_calendar = ak.tool_trade_date_hist_sina()
        trade_dates_set = set(df_calendar['trade_date'].tolist())
        return check_date.strftime('%Y-%m-%d') in trade_dates_set
    except Exception as e:
        print(f"在线获取交易日历失败，降级为仅排除周末: {e}")
        return True                         # 降级：只要不是周末就当作交易日

def save_last_run():
    today_str = date.today().strftime("%Y-%m-%d")
    with open(LAST_RUN_FILE, 'w') as f:
        f.write(today_str)

# ---------- 扫描器类 ----------
class AutoStockScanner:
    def __init__(self, lookback_days=620):
        self.lookback_days = lookback_days
        self.all_stocks = []
        self.stock_name_dict = {}
        self.results = []
        self._init_db()

    def _init_db(self):
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS scan_results (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                scan_date TEXT NOT NULL,
                symbol TEXT NOT NULL,
                name TEXT,
                close REAL,
                ma5 REAL,
                ma20 REAL,
                ma60 REAL,
                rsi REAL,
                cond_met INTEGER,
                score REAL,
                roe REAL,
                profit_growth REAL,
                pe REAL,
                main_inflow REAL,
                update_time TEXT
            )
        ''')
        # 兼容旧表结构，新增列（若已有则忽略报错）
        for col in ['roe', 'profit_growth', 'pe', 'main_inflow']:
            try:
                cursor.execute(f"ALTER TABLE scan_results ADD COLUMN {col} REAL")
            except:
                pass
        conn.commit()
        conn.close()

    def get_all_stock_list(self):
        print("正在获取A股全市场股票列表...")
        try:
            df = ak.stock_info_a_code_name()
            self.all_stocks = df['code'].tolist()
            self.stock_name_dict = dict(zip(df['code'], df['name']))
            print(f"获取到 {len(self.all_stocks)} 只股票")
            return True
        except Exception as e:
            print(f"获取股票列表失败: {e}")
            return False

    def fetch_stock_history(self, symbol):
        try:
            df = ak.stock_zh_a_hist(
                symbol=symbol,
                period="daily",
                start_date=(datetime.now() - timedelta(days=self.lookback_days*2)).strftime("%Y%m%d"),
                end_date=datetime.now().strftime("%Y%m%d"),
                adjust="qfq"
            )
            if df is None or df.empty or len(df) < 60:
                return None
            df = df.rename(columns={"日期":"date","开盘":"open","最高":"high",
                                    "最低":"low","收盘":"close","成交量":"volume"})
            df["date"] = pd.to_datetime(df["date"])
            df = df.sort_values("date").reset_index(drop=True)
            return df[["date","open","high","low","close","volume"]]
        except:
            return None

    def calculate_indicators(self, df):
        if df is None or len(df) < 60:
            return None
        try:
            sdf = wrap(df.copy())
            sdf['ma5'] = sdf['close'].rolling(5).mean()
            sdf['ma20'] = sdf['close'].rolling(20).mean()
            sdf['ma60'] = sdf['close'].rolling(60).mean()
            sdf.get('macd')
            sdf['rsi'] = sdf['rsi_14']
            sdf['vol_ma5'] = sdf['volume'].rolling(5).mean()
            sdf['vol_ma20'] = sdf['volume'].rolling(20).mean()
            return sdf
        except:
            return None

    def get_fundamental_data(self, symbol):
        try:
            df = ak.stock_financial_analysis_indicator(symbol=symbol)
            if df is None or df.empty:
                return None, None, None
            latest = df.iloc[-1]
            roe = latest.get('净资产收益率', None)
            profit_growth = latest.get('净利润增长率', None)
            pe = latest.get('市盈率', None)
            return (
                float(roe) if pd.notna(roe) else None,
                float(profit_growth) if pd.notna(profit_growth) else None,
                float(pe) if pd.notna(pe) else None
            )
        except:
            return None, None, None

    def get_fund_flow(self, symbol):
        try:
            market = "sh" if symbol.startswith('6') else "sz"
            df = ak.stock_individual_fund_flow(stock=symbol, market=market)
            if df is None or df.empty:
                return None
            latest = df.iloc[-1]
            main_inflow = latest.get('主力净流入', None)
            return float(main_inflow) if pd.notna(main_inflow) else None
        except:
            return None

    def evaluate_stock(self, symbol, sdf):
        if sdf is None or len(sdf) < 60:
            return None
        name = self.stock_name_dict.get(symbol, '')
        if 'ST' in name or '*ST' in name:
            return None

        latest = sdf.iloc[-1]

        # 股价过滤：只推荐30元以下股票
        if latest['close'] > 30:
            return None

        # 均线多头排列
        ma5, ma20, ma60 = latest['ma5'], latest['ma20'], latest['ma60']
        if pd.isna(ma5) or pd.isna(ma20) or pd.isna(ma60):
            return None
        c_ma = (ma5 > ma20) and (ma20 > ma60)

        # MACD金叉
        macd, macds, macdh = latest['macd'], latest['macds'], latest['macdh']
        if pd.isna(macd) or pd.isna(macds) or pd.isna(macdh):
            return None
        c_macd = (macd > macds) and (macdh > 0)

        # RSI 强势区间
        rsi = latest['rsi']
        if pd.isna(rsi):
            return None
        c_rsi = 50 < rsi < 75

        # 成交量放大
        vol_ma5, vol_ma20 = latest['vol_ma5'], latest['vol_ma20']
        if pd.isna(vol_ma5) or pd.isna(vol_ma20):
            c_vol = False
        else:
            c_vol = vol_ma5 > vol_ma20 * 1.2

        cond_met = sum([c_ma, c_macd, c_rsi])

        if cond_met >= 2 and c_vol:
            # ---------- 技术面得分 (0–40) ----------
            tech_score = 0
            if c_ma:
                tech_score += (ma5 / ma60 - 1) * 100 * 2
            if c_macd:
                tech_score += abs(macdh) * 10
            if c_rsi:
                tech_score += (rsi - 50) * 0.5
            if len(sdf) >= 2:
                pct = (latest['close'] - sdf.iloc[-2]['close']) / sdf.iloc[-2]['close'] * 100
                tech_score += pct
            # 归一化到0-40
            tech_score = min(max(tech_score, 0), 40) / 40 * 40
            tech_score = round(tech_score, 1)

            # ---------- 基本面得分 (0–30) ----------
            roe, profit_growth, pe = self.get_fundamental_data(symbol)
            fundamental_score = 0
            if roe is not None and roe > 15:
                fundamental_score += 10
            if profit_growth is not None and profit_growth > 0:
                fundamental_score += 10
            if pe is not None and 0 < pe < 30:
                fundamental_score += 10

            # ---------- 资金面得分 (0–30) ----------
            main_inflow = self.get_fund_flow(symbol)
            fund_score = 30 if (main_inflow is not None and main_inflow > 0) else 0

            total_score = tech_score + fundamental_score + fund_score

            return {
                'symbol': symbol,
                'name': name,
                'close': round(latest['close'], 2),
                'ma5': round(ma5, 2),
                'ma20': round(ma20, 2),
                'ma60': round(ma60, 2),
                'rsi': round(rsi, 2),
                'cond_met': cond_met,
                'score': round(total_score, 1),
                'roe': round(roe, 2) if roe is not None else None,
                'profit_growth': round(profit_growth, 2) if profit_growth is not None else None,
                'pe': round(pe, 2) if pe is not None else None,
                'main_inflow': round(main_inflow, 2) if main_inflow is not None else None,
            }
        return None

    def scan_all(self):
        if not self.all_stocks:
            if not self.get_all_stock_list():
                return

        print(f"\n开始全市场扫描，共 {len(self.all_stocks)} 只股票，间隔 0.8 秒...")
        self.results = []
        total = len(self.all_stocks)

        for i, symbol in enumerate(self.all_stocks):
            if (i + 1) % 100 == 0:
                print(f"进度: {i+1}/{total}")

            name = self.stock_name_dict.get(symbol, '')
            if 'ST' in name or '*ST' in name:
                continue

            df = self.fetch_stock_history(symbol)
            if df is not None:
                sdf = self.calculate_indicators(df)
                if sdf is not None:
                    res = self.evaluate_stock(symbol, sdf)
                    if res:
                        self.results.append(res)

            time.sleep(0.8)

        self.results.sort(key=lambda x: x['score'], reverse=True)
        print(f"\n扫描完成！符合条件股票数: {len(self.results)}")
        return self.results

    def save_to_db(self):
        if not self.results:
            print("无结果可保存")
            return

        today = datetime.now().strftime("%Y-%m-%d")
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute("DELETE FROM scan_results WHERE scan_date = ?", (today,))

        for r in self.results:
            cursor.execute('''
                INSERT INTO scan_results 
                (scan_date, symbol, name, close, ma5, ma20, ma60, rsi, cond_met, score,
                 roe, profit_growth, pe, main_inflow, update_time)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                today, r['symbol'], r['name'], r['close'],
                r['ma5'], r['ma20'], r['ma60'], r['rsi'],
                r['cond_met'], r['score'],
                r['roe'], r['profit_growth'], r['pe'], r['main_inflow'], now
            ))

        conn.commit()
        conn.close()
        print(f"结果已存入数据库，日期: {today}")

# ---------- 主逻辑（云端简化版，不再等待、不自行推送）----------
def main():
    # 检查是否交易日（非交易日直接退出）
    if not is_trade_day():
        print("今天非交易日，脚本退出。")
        sys.exit(0)

    print("=" * 60)
    print(f"开始执行扫描 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 60)

    scanner = AutoStockScanner(lookback_days=620)
    scanner.scan_all()
    scanner.save_to_db()

    # 记录本次扫描日期
    save_last_run()

    print("任务完成，等待工作流提交。")

if __name__ == "__main__":
    main()