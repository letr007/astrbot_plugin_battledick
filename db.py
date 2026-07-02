import sqlite3
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

    def _init_db(self) -> None:
        Path(self.db_path).parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(self.db_path)
        cursor = self.conn.cursor()
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS user_dick_stats (
                user_id TEXT PRIMARY KEY,
                user_name TEXT,
                length REAL DEFAULT 0
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS user_milk_stats (
                user_id TEXT PRIMARY KEY,
                user_name TEXT,
                milk_ml REAL DEFAULT 0
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS user_daily_growth (
                user_id TEXT PRIMARY KEY,
                date TEXT,
                count INTEGER DEFAULT 0
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS user_daily_lu (
                user_id TEXT PRIMARY KEY,
                date TEXT,
                count INTEGER DEFAULT 0
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS user_growth_state (
                user_id TEXT PRIMARY KEY,
                last_growth_date TEXT
            )
            """
        )
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS user_profiles (
                user_id TEXT PRIMARY KEY,
                user_name TEXT
            )
            """
        )
        cursor.execute(
            """
            INSERT OR IGNORE INTO user_profiles (user_id, user_name)
            SELECT user_id, user_name FROM user_dick_stats
            WHERE user_name IS NOT NULL AND user_name != ''
            """
        )
        self.conn.commit()

    def get_user_length(self, user_id: str) -> float:
        self._ensure_conn()
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT length FROM user_dick_stats WHERE user_id = ?",
            (user_id,),
        )
        result = cursor.fetchone()
        if not result:
            return 0.0
        return self._round_length(result[0])

    def get_user_profile_name(self, user_id: str) -> str:
        self._ensure_conn()
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT user_name FROM user_profiles WHERE user_id = ?",
            (user_id,),
        )
        result = cursor.fetchone()
        if not result or not result[0]:
            return ""
        return str(result[0])

    def get_user_name(self, user_id: str) -> str:
        self._ensure_conn()
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT user_name FROM user_dick_stats WHERE user_id = ?",
            (user_id,),
        )
        result = cursor.fetchone()
        if not result or not result[0]:
            return ""
        return str(result[0])

    def upsert_user_profile(self, user_id: str, user_name: str) -> None:
        if not user_id or not user_name:
            return
        self._ensure_conn()
        cursor = self.conn.cursor()
        cursor.execute(
            """
            INSERT INTO user_profiles (user_id, user_name)
            VALUES (?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
            user_name = excluded.user_name
            """,
            (user_id, user_name),
        )
        self.conn.commit()

    def update_user_length(self, user_id: str, user_name: str, length: float) -> None:
        self._ensure_conn()
        safe_length = max(0.0, self._round_length(length))
        cursor = self.conn.cursor()
        cursor.execute(
            """
            INSERT INTO user_dick_stats (user_id, user_name, length)
            VALUES (?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
            length = excluded.length,
            user_name = excluded.user_name
            """,
            (user_id, user_name, safe_length),
        )
        self.conn.commit()
        self.upsert_user_profile(user_id, user_name)

    def adjust_user_length(
        self,
        user_id: str,
        delta: float,
        user_name: str = "",
    ) -> float:
        current_length = self.get_user_length(user_id)
        new_length = max(0.0, current_length + delta)
        self.update_user_length(user_id, user_name, new_length)
        return self._round_length(new_length)

    def get_user_milk(self, user_id: str) -> float:
        self._ensure_conn()
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT milk_ml FROM user_milk_stats WHERE user_id = ?",
            (user_id,),
        )
        result = cursor.fetchone()
        if not result:
            return 0.0
        return self._round_length(result[0])

    def adjust_user_milk(
        self, user_id: str, delta: float, user_name: str = ""
    ) -> float:
        self._ensure_conn()
        cursor = self.conn.cursor()
        cursor.execute(
            "SELECT milk_ml FROM user_milk_stats WHERE user_id = ?",
            (user_id,),
        )
        result = cursor.fetchone()

        current_milk = float(result[0]) if result else 0.0
        new_milk = max(0.0, current_milk + delta)
        new_milk = self._round_length(new_milk)

        cursor.execute(
            """
            INSERT INTO user_milk_stats (user_id, user_name, milk_ml)
            VALUES (?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
            milk_ml = excluded.milk_ml,
            user_name = excluded.user_name
            """,
            (user_id, user_name, new_milk),
        )
        self.conn.commit()
        self.upsert_user_profile(user_id, user_name)
        return new_milk

    def get_daily_growth_count(self, user_id: str, date_str: str) -> int:
        return self._get_daily_count("user_daily_growth", user_id, date_str)

    def increment_daily_growth(self, user_id: str, date_str: str) -> int:
        return self._increment_daily_count("user_daily_growth", user_id, date_str)

    def get_daily_lu_count(self, user_id: str, date_str: str) -> int:
        return self._get_daily_count("user_daily_lu", user_id, date_str)

    def increment_daily_lu(self, user_id: str, date_str: str) -> int:
        return self._increment_daily_count("user_daily_lu", user_id, date_str)

    def _get_daily_count(self, table: str, user_id: str, date_str: str) -> int:
        self._ensure_conn()
        cursor = self.conn.cursor()
        cursor.execute(
            f"SELECT date, count FROM {table} WHERE user_id = ?",
            (user_id,),
        )
        result = cursor.fetchone()
        if not result:
            return 0

        last_date, count = result
        if last_date != date_str:
            return 0
        return int(count or 0)

    def _increment_daily_count(self, table: str, user_id: str, date_str: str) -> int:
        self._ensure_conn()
        cursor = self.conn.cursor()
        cursor.execute(
            f"SELECT date, count FROM {table} WHERE user_id = ?",
            (user_id,),
        )
        result = cursor.fetchone()
        if not result or result[0] != date_str:
            count = 1
        else:
            count = int(result[1] or 0) + 1

        cursor.execute(
            f"""
            INSERT INTO {table} (user_id, date, count)
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

    def close(self) -> None:
        if self.conn:
            self.conn.close()
