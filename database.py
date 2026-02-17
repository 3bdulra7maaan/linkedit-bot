"""
LinkedIt Bot - Database Module
SQLite database for user profiles, favorites, job alerts, and analytics.
Designed to work on Render with persistent disk or ephemeral storage.
"""

import sqlite3
import json
import logging
import os
from datetime import datetime
from contextlib import contextmanager

logger = logging.getLogger(__name__)

# Database file path - can be overridden via env var for persistent storage on Render
DB_PATH = os.environ.get("DB_PATH", "linkedit.db")


@contextmanager
def get_db():
    """Thread-safe database connection context manager."""
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")  # Better concurrency
    conn.execute("PRAGMA foreign_keys=ON")
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def init_db():
    """Initialize database tables."""
    with get_db() as conn:
        conn.executescript("""
            -- User profiles table
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                preferred_countries TEXT DEFAULT '[]',
                preferred_keywords TEXT DEFAULT '[]',
                alerts_enabled INTEGER DEFAULT 0,
                alert_interval TEXT DEFAULT 'daily',
                language TEXT DEFAULT 'ar',
                created_at TEXT DEFAULT (datetime('now')),
                updated_at TEXT DEFAULT (datetime('now'))
            );

            -- Saved/favorite jobs table
            CREATE TABLE IF NOT EXISTS favorites (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                job_title TEXT,
                company TEXT,
                location TEXT,
                job_url TEXT,
                email TEXT,
                source TEXT,
                country_name TEXT,
                description TEXT,
                saved_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (user_id) REFERENCES users(user_id)
            );

            -- Job alerts table
            CREATE TABLE IF NOT EXISTS alerts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                keyword TEXT NOT NULL,
                country_code TEXT DEFAULT 'all',
                is_active INTEGER DEFAULT 1,
                last_sent TEXT,
                created_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (user_id) REFERENCES users(user_id)
            );

            -- Sent jobs tracking (to avoid duplicates in alerts)
            CREATE TABLE IF NOT EXISTS sent_jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                job_url TEXT NOT NULL,
                sent_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (user_id) REFERENCES users(user_id),
                UNIQUE(user_id, job_url)
            );

            -- Search history tracking (for analytics)
            CREATE TABLE IF NOT EXISTS search_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                search_term TEXT NOT NULL,
                country_code TEXT DEFAULT 'all',
                results_count INTEGER DEFAULT 0,
                source TEXT DEFAULT 'search',
                searched_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY (user_id) REFERENCES users(user_id)
            );

            -- Bot activity log (daily aggregates)
            CREATE TABLE IF NOT EXISTS daily_stats (
                date TEXT PRIMARY KEY,
                new_users INTEGER DEFAULT 0,
                total_searches INTEGER DEFAULT 0,
                total_favorites INTEGER DEFAULT 0,
                total_alerts_sent INTEGER DEFAULT 0
            );

            -- Create indexes for performance
            CREATE INDEX IF NOT EXISTS idx_favorites_user ON favorites(user_id);
            CREATE INDEX IF NOT EXISTS idx_alerts_user ON alerts(user_id);
            CREATE INDEX IF NOT EXISTS idx_alerts_active ON alerts(is_active);
            CREATE INDEX IF NOT EXISTS idx_sent_jobs_user ON sent_jobs(user_id);
            CREATE INDEX IF NOT EXISTS idx_search_history_user ON search_history(user_id);
            CREATE INDEX IF NOT EXISTS idx_search_history_term ON search_history(search_term);
            CREATE INDEX IF NOT EXISTS idx_search_history_date ON search_history(searched_at);
            CREATE INDEX IF NOT EXISTS idx_search_history_country ON search_history(country_code);
        """)
    logger.info("Database initialized at: %s", DB_PATH)


# ========================
# User Profile Functions
# ========================

def get_or_create_user(user_id: int, username: str = "", first_name: str = "") -> dict:
    """Get existing user or create new one."""
    with get_db() as conn:
        row = conn.execute("SELECT * FROM users WHERE user_id = ?", (user_id,)).fetchone()
        if row:
            # Update username/first_name if changed
            if (username and username != row["username"]) or (first_name and first_name != row["first_name"]):
                conn.execute(
                    "UPDATE users SET username = ?, first_name = ?, updated_at = datetime('now') WHERE user_id = ?",
                    (username or row["username"], first_name or row["first_name"], user_id),
                )
            return dict(row)
        conn.execute(
            "INSERT INTO users (user_id, username, first_name) VALUES (?, ?, ?)",
            (user_id, username or "", first_name or ""),
        )
        # Track new user in daily stats
        _increment_daily_stat("new_users", conn)
        return {
            "user_id": user_id,
            "username": username,
            "first_name": first_name,
            "preferred_countries": "[]",
            "preferred_keywords": "[]",
            "alerts_enabled": 0,
            "alert_interval": "daily",
        }


def update_user_preferences(user_id: int, countries: list = None, keywords: list = None):
    """Update user's preferred countries and keywords."""
    with get_db() as conn:
        if countries is not None:
            conn.execute(
                "UPDATE users SET preferred_countries = ?, updated_at = datetime('now') WHERE user_id = ?",
                (json.dumps(countries), user_id),
            )
        if keywords is not None:
            conn.execute(
                "UPDATE users SET preferred_keywords = ?, updated_at = datetime('now') WHERE user_id = ?",
                (json.dumps(keywords), user_id),
            )


def get_user_preferences(user_id: int) -> dict:
    """Get user's saved preferences."""
    with get_db() as conn:
        row = conn.execute("SELECT * FROM users WHERE user_id = ?", (user_id,)).fetchone()
        if not row:
            return {"preferred_countries": [], "preferred_keywords": []}
        return {
            "preferred_countries": json.loads(row["preferred_countries"] or "[]"),
            "preferred_keywords": json.loads(row["preferred_keywords"] or "[]"),
            "alerts_enabled": bool(row["alerts_enabled"]),
            "alert_interval": row["alert_interval"],
        }


# ========================
# Favorites Functions
# ========================

def save_favorite(user_id: int, job: dict) -> bool:
    """Save a job to user's favorites. Returns False if already saved."""
    job_url = str(job.get("job_url", ""))
    with get_db() as conn:
        existing = conn.execute(
            "SELECT id FROM favorites WHERE user_id = ? AND job_url = ?",
            (user_id, job_url),
        ).fetchone()
        if existing:
            return False

        conn.execute(
            """INSERT INTO favorites (user_id, job_title, company, location, job_url, email, source, country_name, description)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                user_id,
                str(job.get("title", "")),
                str(job.get("company", "")),
                str(job.get("location", "")),
                job_url,
                str(job.get("_email", "")),
                str(job.get("site", "")),
                str(job.get("_country_name", "")),
                str(job.get("description", ""))[:500],
            ),
        )
        _increment_daily_stat("total_favorites", conn)
        return True


def get_favorites(user_id: int, limit: int = 20) -> list:
    """Get user's saved favorite jobs."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT * FROM favorites WHERE user_id = ? ORDER BY saved_at DESC LIMIT ?",
            (user_id, limit),
        ).fetchall()
        return [dict(r) for r in rows]


def remove_favorite(user_id: int, favorite_id: int) -> bool:
    """Remove a job from favorites."""
    with get_db() as conn:
        cursor = conn.execute(
            "DELETE FROM favorites WHERE id = ? AND user_id = ?",
            (favorite_id, user_id),
        )
        return cursor.rowcount > 0


def count_favorites(user_id: int) -> int:
    """Count user's favorites."""
    with get_db() as conn:
        row = conn.execute(
            "SELECT COUNT(*) as cnt FROM favorites WHERE user_id = ?", (user_id,)
        ).fetchone()
        return row["cnt"] if row else 0


# ========================
# Alert Functions
# ========================

def add_alert(user_id: int, keyword: str, country_code: str = "all") -> int:
    """Add a new job alert. Returns alert ID."""
    with get_db() as conn:
        existing = conn.execute(
            "SELECT id FROM alerts WHERE user_id = ? AND keyword = ? AND country_code = ? AND is_active = 1",
            (user_id, keyword.lower().strip(), country_code),
        ).fetchone()
        if existing:
            return -1

        cursor = conn.execute(
            "INSERT INTO alerts (user_id, keyword, country_code) VALUES (?, ?, ?)",
            (user_id, keyword.lower().strip(), country_code),
        )
        conn.execute(
            "UPDATE users SET alerts_enabled = 1, updated_at = datetime('now') WHERE user_id = ?",
            (user_id,),
        )
        return cursor.lastrowid


def get_user_alerts(user_id: int) -> list:
    """Get all active alerts for a user."""
    with get_db() as conn:
        rows = conn.execute(
            "SELECT * FROM alerts WHERE user_id = ? AND is_active = 1 ORDER BY created_at DESC",
            (user_id,),
        ).fetchall()
        return [dict(r) for r in rows]


def remove_alert(user_id: int, alert_id: int) -> bool:
    """Deactivate an alert."""
    with get_db() as conn:
        cursor = conn.execute(
            "UPDATE alerts SET is_active = 0 WHERE id = ? AND user_id = ?",
            (alert_id, user_id),
        )
        remaining = conn.execute(
            "SELECT COUNT(*) as cnt FROM alerts WHERE user_id = ? AND is_active = 1",
            (user_id,),
        ).fetchone()
        if remaining and remaining["cnt"] == 0:
            conn.execute(
                "UPDATE users SET alerts_enabled = 0, updated_at = datetime('now') WHERE user_id = ?",
                (user_id,),
            )
        return cursor.rowcount > 0


def get_all_active_alerts() -> list:
    """Get all active alerts across all users (for the alert scheduler)."""
    with get_db() as conn:
        rows = conn.execute(
            """SELECT a.*, u.first_name, u.username
               FROM alerts a
               JOIN users u ON a.user_id = u.user_id
               WHERE a.is_active = 1""",
        ).fetchall()
        return [dict(r) for r in rows]


def update_alert_sent(alert_id: int):
    """Update the last_sent timestamp for an alert."""
    with get_db() as conn:
        conn.execute(
            "UPDATE alerts SET last_sent = datetime('now') WHERE id = ?",
            (alert_id,),
        )
        _increment_daily_stat("total_alerts_sent", conn)


def is_job_sent(user_id: int, job_url: str) -> bool:
    """Check if a job was already sent to a user."""
    with get_db() as conn:
        row = conn.execute(
            "SELECT id FROM sent_jobs WHERE user_id = ? AND job_url = ?",
            (user_id, job_url),
        ).fetchone()
        return row is not None


def mark_job_sent(user_id: int, job_url: str):
    """Mark a job as sent to a user."""
    with get_db() as conn:
        try:
            conn.execute(
                "INSERT OR IGNORE INTO sent_jobs (user_id, job_url) VALUES (?, ?)",
                (user_id, job_url),
            )
        except sqlite3.IntegrityError:
            pass


def count_alerts(user_id: int) -> int:
    """Count user's active alerts."""
    with get_db() as conn:
        row = conn.execute(
            "SELECT COUNT(*) as cnt FROM alerts WHERE user_id = ? AND is_active = 1",
            (user_id,),
        ).fetchone()
        return row["cnt"] if row else 0


# ========================
# Search Tracking
# ========================

def log_search(user_id: int, search_term: str, country_code: str, results_count: int, source: str = "search"):
    """Log a search for analytics. Auto-creates user if not exists to avoid FOREIGN KEY errors."""
    with get_db() as conn:
        # Ensure user exists to prevent FOREIGN KEY constraint failure
        existing = conn.execute("SELECT 1 FROM users WHERE user_id = ?", (user_id,)).fetchone()
        if not existing:
            conn.execute(
                "INSERT OR IGNORE INTO users (user_id, username, first_name) VALUES (?, ?, ?)",
                (user_id, "", ""),
            )
        conn.execute(
            "INSERT INTO search_history (user_id, search_term, country_code, results_count, source) VALUES (?, ?, ?, ?, ?)",
            (user_id, search_term.lower().strip(), country_code, results_count, source),
        )
        _increment_daily_stat("total_searches", conn)


# ========================
# Daily Stats Helper
# ========================

def _increment_daily_stat(field: str, conn=None):
    """Increment a daily stat counter. Can use existing connection or create new one."""
    today = datetime.utcnow().strftime("%Y-%m-%d")
    if conn:
        conn.execute(
            f"""INSERT INTO daily_stats (date, {field}) VALUES (?, 1)
                ON CONFLICT(date) DO UPDATE SET {field} = {field} + 1""",
            (today,),
        )
    else:
        with get_db() as new_conn:
            new_conn.execute(
                f"""INSERT INTO daily_stats (date, {field}) VALUES (?, 1)
                    ON CONFLICT(date) DO UPDATE SET {field} = {field} + 1""",
                (today,),
            )


# ========================
# Admin Statistics & Analytics
# ========================

def get_bot_stats() -> dict:
    """Get basic bot statistics."""
    with get_db() as conn:
        users = conn.execute("SELECT COUNT(*) as cnt FROM users").fetchone()["cnt"]
        favorites = conn.execute("SELECT COUNT(*) as cnt FROM favorites").fetchone()["cnt"]
        alerts = conn.execute("SELECT COUNT(*) as cnt FROM alerts WHERE is_active = 1").fetchone()["cnt"]
        searches = conn.execute("SELECT COUNT(*) as cnt FROM search_history").fetchone()["cnt"]
        return {
            "total_users": users,
            "total_favorites": favorites,
            "active_alerts": alerts,
            "total_searches": searches,
        }


def get_admin_overview() -> dict:
    """Get comprehensive admin overview statistics."""
    with get_db() as conn:
        # Total counts
        total_users = conn.execute("SELECT COUNT(*) as cnt FROM users").fetchone()["cnt"]
        total_favorites = conn.execute("SELECT COUNT(*) as cnt FROM favorites").fetchone()["cnt"]
        active_alerts = conn.execute("SELECT COUNT(*) as cnt FROM alerts WHERE is_active = 1").fetchone()["cnt"]
        total_searches = conn.execute("SELECT COUNT(*) as cnt FROM search_history").fetchone()["cnt"]
        total_sent_jobs = conn.execute("SELECT COUNT(*) as cnt FROM sent_jobs").fetchone()["cnt"]

        # Today's stats
        today = datetime.utcnow().strftime("%Y-%m-%d")
        today_row = conn.execute("SELECT * FROM daily_stats WHERE date = ?", (today,)).fetchone()
        today_stats = dict(today_row) if today_row else {"new_users": 0, "total_searches": 0, "total_favorites": 0, "total_alerts_sent": 0}

        # Users today
        users_today = conn.execute(
            "SELECT COUNT(*) as cnt FROM users WHERE date(created_at) = ?", (today,)
        ).fetchone()["cnt"]

        # Users this week
        users_week = conn.execute(
            "SELECT COUNT(*) as cnt FROM users WHERE created_at >= datetime('now', '-7 days')"
        ).fetchone()["cnt"]

        # Searches today
        searches_today = conn.execute(
            "SELECT COUNT(*) as cnt FROM search_history WHERE date(searched_at) = ?", (today,)
        ).fetchone()["cnt"]

        # Searches this week
        searches_week = conn.execute(
            "SELECT COUNT(*) as cnt FROM search_history WHERE searched_at >= datetime('now', '-7 days')"
        ).fetchone()["cnt"]

        return {
            "total_users": total_users,
            "total_favorites": total_favorites,
            "active_alerts": active_alerts,
            "total_searches": total_searches,
            "total_sent_jobs": total_sent_jobs,
            "users_today": users_today,
            "users_this_week": users_week,
            "searches_today": searches_today,
            "searches_this_week": searches_week,
            "today_stats": today_stats,
        }


def get_top_searches(limit: int = 10) -> list:
    """Get most popular search terms."""
    with get_db() as conn:
        rows = conn.execute(
            """SELECT search_term, COUNT(*) as count, 
                      MAX(searched_at) as last_searched,
                      AVG(results_count) as avg_results
               FROM search_history
               GROUP BY search_term
               ORDER BY count DESC
               LIMIT ?""",
            (limit,),
        ).fetchall()
        return [dict(r) for r in rows]


def get_top_countries(limit: int = 10) -> list:
    """Get most searched countries."""
    with get_db() as conn:
        rows = conn.execute(
            """SELECT country_code, COUNT(*) as count
               FROM search_history
               GROUP BY country_code
               ORDER BY count DESC
               LIMIT ?""",
            (limit,),
        ).fetchall()
        return [dict(r) for r in rows]


def get_active_users(limit: int = 10) -> list:
    """Get most active users by search count."""
    with get_db() as conn:
        rows = conn.execute(
            """SELECT u.user_id, u.username, u.first_name, u.created_at,
                      COUNT(sh.id) as search_count,
                      (SELECT COUNT(*) FROM favorites f WHERE f.user_id = u.user_id) as fav_count,
                      (SELECT COUNT(*) FROM alerts a WHERE a.user_id = u.user_id AND a.is_active = 1) as alert_count
               FROM users u
               LEFT JOIN search_history sh ON u.user_id = sh.user_id
               GROUP BY u.user_id
               ORDER BY search_count DESC
               LIMIT ?""",
            (limit,),
        ).fetchall()
        return [dict(r) for r in rows]


def get_recent_users(limit: int = 10) -> list:
    """Get most recently joined users."""
    with get_db() as conn:
        rows = conn.execute(
            """SELECT user_id, username, first_name, created_at
               FROM users
               ORDER BY created_at DESC
               LIMIT ?""",
            (limit,),
        ).fetchall()
        return [dict(r) for r in rows]


def get_daily_stats_history(days: int = 7) -> list:
    """Get daily stats for the last N days."""
    with get_db() as conn:
        rows = conn.execute(
            """SELECT * FROM daily_stats
               WHERE date >= date('now', ? || ' days')
               ORDER BY date DESC""",
            (f"-{days}",),
        ).fetchall()
        return [dict(r) for r in rows]


def get_search_history_for_user(user_id: int, limit: int = 10) -> list:
    """Get search history for a specific user."""
    with get_db() as conn:
        rows = conn.execute(
            """SELECT search_term, country_code, results_count, searched_at
               FROM search_history
               WHERE user_id = ?
               ORDER BY searched_at DESC
               LIMIT ?""",
            (user_id, limit),
        ).fetchall()
        return [dict(r) for r in rows]


def get_hourly_search_distribution() -> list:
    """Get search distribution by hour of day."""
    with get_db() as conn:
        rows = conn.execute(
            """SELECT strftime('%H', searched_at) as hour, COUNT(*) as count
               FROM search_history
               GROUP BY hour
               ORDER BY hour""",
        ).fetchall()
        return [dict(r) for r in rows]


def get_zero_result_searches(limit: int = 10) -> list:
    """Get searches that returned zero results (useful for improving the bot)."""
    with get_db() as conn:
        rows = conn.execute(
            """SELECT search_term, country_code, COUNT(*) as count
               FROM search_history
               WHERE results_count = 0
               GROUP BY search_term, country_code
               ORDER BY count DESC
               LIMIT ?""",
            (limit,),
        ).fetchall()
        return [dict(r) for r in rows]


def broadcast_get_all_user_ids() -> list:
    """Get all user IDs for broadcast messages."""
    with get_db() as conn:
        rows = conn.execute("SELECT user_id FROM users").fetchall()
        return [r["user_id"] for r in rows]
