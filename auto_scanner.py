# auto_scanner.py
import akshare as ak
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from stockstats import wrap
import time
import warnings
import sqlite3
import os
import sys
import subprocess

warnings.filterwarnings('ignore')

# ---------- 配置 ----------
DB_PATH = os.path.join(os.path.dirname(__file__), "stock_scan.db")
LOCK_FILE = os.path.join(os.path.dirname(__file__), "scanner.lock")

# ---------- 防止重复运行 ----------
def check_single_instance():
    if os.path.exists(LOCK_FILE):
        try:
            mtime = os.path.getmtime(LOCK_FILE)
            if time.time() - mtime < 86400:
                print("已有扫描进程在运行，退出。")
                sys.exit(0)
            else:
                os.remove(LOCK_FILE)
        except:
            pass
    with open(LOCK_FILE, 'w') as f:
        f.write(str(os.getpid()))

def release_lock():
    try:
        if os.path.exists(LOCK_FILE):
            os.remove(LOCK_FILE)
    except:
        pass

# ---------- 扫描器类（与之前相同）----------
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
                update_time TEXT
            )
        ''')
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
            df = df.rename(columns={"日期":"date","开盘":"open","最高":"high","最低":"low","收盘":"close","成交量":"volume"})
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

    def evaluate_stock(self, symbol, sdf):
        if sdf is None or len(sdf) < 60:
            return None
        name = self.stock_name_dict.get(symbol, '')
        if 'ST' in name or '*ST' in name:
            return None

        latest = sdf.iloc[-1]
        ma5, ma20, ma60 = latest['ma5'], latest['ma20'], latest['ma60']
        if pd.isna(ma5) or pd.isna(ma20) or pd.isna(ma60):
            return None
        c_ma = (ma5 > ma20) and (ma20 > ma60)

        macd, macds, macdh = latest['macd'], latest['macds'], latest['macdh']
        if pd.isna(macd) or pd.isna(macds) or pd.isna(macdh):
            return None
        c_macd = (macd > macds) and (macdh > 0)

        rsi = latest['rsi']
        if pd.isna(rsi):
            return None
        c_rsi = 50 < rsi < 75

        vol_ma5, vol_ma20 = latest['vol_ma5'], latest['vol_ma20']
        if pd.isna(vol_ma5) or pd.isna(vol_ma20):
            c_vol = False
        else:
            c_vol = vol_ma5 > vol_ma20 * 1.2

        cond_met = sum([c_ma, c_macd, c_rsi])

        if cond_met >= 2 and c_vol:
            score = 0
            if c_ma:
                score += (ma5 / ma60 - 1) * 100 * 2
            if c_macd:
                score += abs(macdh) * 10
            if c_rsi:
                score += (rsi - 50) * 0.5
            if len(sdf) >= 2:
                pct = (latest['close'] - sdf.iloc[-2]['close']) / sdf.iloc[-2]['close'] * 100
                score += pct

            return {
                'symbol': symbol,
                'name': name,
                'close': round(latest['close'], 2),
                'ma5': round(ma5, 2),
                'ma20': round(ma20, 2),
                'ma60': round(ma60, 2),
                'rsi': round(rsi, 2),
                'cond_met': cond_met,
                'score': round(score, 2),
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
                (scan_date, symbol, name, close, ma5, ma20, ma60, rsi, cond_met, score, update_time)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (
                today, r['symbol'], r['name'], r['close'],
                r['ma5'], r['ma20'], r['ma60'], r['rsi'],
                r['cond_met'], r['score'], now
            ))

        conn.commit()
        conn.close()
        print(f"结果已存入数据库，日期: {today}")

# ---------- Git 自动推送 ----------
def git_push():
    """将数据库文件提交并推送到 GitHub 仓库"""
    try:
        print("正在推送数据库到 GitHub...")
        # 切换到脚本所在目录（即仓库根目录）
        os.chdir(os.path.dirname(os.path.abspath(__file__)))
        subprocess.run(["git", "add", "stock_scan.db"], check=True)
        subprocess.run(["git", "commit", "-m", f"update db {datetime.now().strftime('%Y%m%d')}"], check=True)
        subprocess.run(["git", "push"], check=True)
        print("推送成功！")
    except subprocess.CalledProcessError as e:
        print(f"Git 推送失败：{e}")
        print("如果提示认证失败，请检查 Personal Access Token 是否已保存。")
        # 尝试用 Token 推送（需要配置远程仓库地址，暂时忽略）

# ---------- 主逻辑 ----------
def wait_until_17():
    return 0

def main():
    check_single_instance()
    try:
        wait_sec = wait_until_17()
        if wait_sec > 0:
            time.sleep(wait_sec)

        print("=" * 60)
        print(f"开始执行扫描 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 60)

        scanner = AutoStockScanner(lookback_days=620)
        scanner.scan_all()
        scanner.save_to_db()
        # 扫描完成后自动推送数据库到 GitHub
        git_push()

        print("任务完成，脚本即将退出。")
    except Exception as e:
        print(f"运行出错: {e}")
    finally:
        release_lock()
        time.sleep(3)
        sys.exit(0)

if __name__ == "__main__":
    main()