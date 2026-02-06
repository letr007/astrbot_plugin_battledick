import sqlite3
import os
from pathlib import Path

class Database:
    def __init__(self, db_path: str):
        self.db_path = db_path
        self.conn = None
        self._init_db()

    @staticmethod
    def _round_length(value: float) -> float:
        return round(float(value), 2)

    def _ensure_conn(self) -> None:
        if self.conn is None:
            self.conn = sqlite3.connect(self.db_path)

    def _init_db(self):
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(self.db_path)
        cursor = self.conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS user_dick_stats (
                user_id TEXT PRIMARY KEY,
                user_name TEXT,
                length REAL DEFAULT 0
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS user_milk_stats (
                user_id TEXT PRIMARY KEY,
                user_name TEXT,
                milk_ml REAL DEFAULT 0
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS user_daily_growth (
                user_id TEXT PRIMARY KEY,
                date TEXT,
                count INTEGER DEFAULT 0
            )
        ''')
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS user_growth_state (
                user_id TEXT PRIMARY KEY,
                last_growth_date TEXT
            )
        ''')
        self.conn.commit()

    def get_user_length(self, user_id: str):
        self._ensure_conn()
        cursor = self.conn.cursor()
        cursor.execute('SELECT length FROM user_dick_stats WHERE user_id = ?', (user_id,))
        result = cursor.fetchone()
        if result:
            return self._round_length(result[0])
        return 0.0

    def update_user_length(self, user_id: str, user_name: str, length: float):
        self._ensure_conn()
        length = self._round_length(length)
        cursor = self.conn.cursor()
        cursor.execute('''
            INSERT INTO user_dick_stats (user_id, user_name, length) 
            VALUES (?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
            length = excluded.length,
            user_name = excluded.user_name
        ''', (user_id, user_name, length))
        self.conn.commit()

    def get_user_milk(self, user_id: str) -> float:
        self._ensure_conn()
        cursor = self.conn.cursor()
        cursor.execute('SELECT milk_ml FROM user_milk_stats WHERE user_id = ?', (user_id,))
        result = cursor.fetchone()
        if result:
            return self._round_length(result[0])
        return 0.0

    def adjust_user_milk(self, user_id: str, delta: float, user_name: str = "") -> float:
        self._ensure_conn()
        cursor = self.conn.cursor()
        cursor.execute('SELECT milk_ml FROM user_milk_stats WHERE user_id = ?', (user_id,))
        result = cursor.fetchone()
        if result:
            new_milk = max(0.0, result[0] + delta)
        else:
            if delta > 0:
                new_milk = delta
                cursor.execute(
                    'INSERT INTO user_milk_stats (user_id, user_name, milk_ml) VALUES (?, ?, ?)',
                    (user_id, user_name, new_milk),
                )
            else:
                new_milk = 0.0
        new_milk = self._round_length(new_milk)
        if result or delta > 0:
            cursor.execute('''
                INSERT INTO user_milk_stats (user_id, user_name, milk_ml)
                VALUES (?, ?, ?)
                ON CONFLICT(user_id) DO UPDATE SET
                milk_ml = excluded.milk_ml,
                user_name = excluded.user_name
            ''', (user_id, user_name, new_milk))
        self.conn.commit()
        return new_milk

    def adjust_user_length(self, user_id: str, delta: float, user_name: str = ""):
        """调整用户长度（正数为增加，负数为减少）"""
        self._ensure_conn()
        cursor = self.conn.cursor()
        cursor.execute('SELECT length FROM user_dick_stats WHERE user_id = ?', (user_id,))
        result = cursor.fetchone()
        if result:
            new_length = max(0.0, result[0] + delta)  # 长度不能为负
        else:
            # 用户不存在，如果 delta 为正则创建，否则忽略（长度保持0）
            if delta > 0:
                new_length = delta
                cursor.execute(
                    'INSERT INTO user_dick_stats (user_id, user_name, length) VALUES (?, ?, ?)',
                    (user_id, user_name, new_length),
                )
            else:
                new_length = 0.0
        new_length = self._round_length(new_length)
        if result or delta > 0:
            cursor.execute('''
                INSERT INTO user_dick_stats (user_id, user_name, length)
                VALUES (?, ?, ?)
                ON CONFLICT(user_id) DO UPDATE SET
                length = excluded.length,
                user_name = excluded.user_name
            ''', (user_id, user_name, new_length))
        self.conn.commit()
        return new_length

    def close(self):
        if self.conn:
            self.conn.close()

    def get_daily_growth_count(self, user_id: str, date_str: str) -> int:
        self._ensure_conn()
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT date, count FROM user_daily_growth WHERE user_id = ?",
            (user_id,),
        )
        result = cursor.fetchone()
        if not result:
            return 0
        last_date, count = result
        if last_date != date_str:
            return 0
        return int(count or 0)

    def increment_daily_growth(self, user_id: str, date_str: str) -> int:
        self._ensure_conn()
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT date, count FROM user_daily_growth WHERE user_id = ?",
            (user_id,),
        )
        result = cursor.fetchone()
        if not result or result[0] != date_str:
            count = 1
        else:
            count = int(result[1] or 0) + 1
        cursor.execute(
            """
            INSERT INTO user_daily_growth (user_id, date, count)
            VALUES (?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
            date = excluded.date,
            count = excluded.count
            """,
            (user_id, date_str, count),
        )
        self.conn.commit()
        return count

    def get_last_growth_date(self, user_id: str) -> str | None:
        self._ensure_conn()
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT last_growth_date FROM user_growth_state WHERE user_id = ?",
            (user_id,),
        )
        result = cursor.fetchone()
        if not result:
            return None
        return result[0]

    def set_last_growth_date(self, user_id: str, date_str: str) -> None:
        self._ensure_conn()
        cursor = self.conn.cursor()
        cursor.execute(
            """
            INSERT INTO user_growth_state (user_id, last_growth_date)
            VALUES (?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
            last_growth_date = excluded.last_growth_date
            """,
            (user_id, date_str),
        )
        self.conn.commit()
