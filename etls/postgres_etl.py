# etls/postgres_etl.py

import psycopg2
import pandas as pd
from psycopg2.extras import execute_values

UPSERT_SQL = """
INSERT INTO lemmy_posts (
  post_id, name, url, body, published, updated,
  community_name, creator_name,
  score, upvotes, downvotes, comments
)
VALUES %s
ON CONFLICT (post_id) DO UPDATE SET
  name = EXCLUDED.name,
  url = EXCLUDED.url,
  body = EXCLUDED.body,
  published = EXCLUDED.published,
  updated = EXCLUDED.updated,
  community_name = EXCLUDED.community_name,
  creator_name = EXCLUDED.creator_name,
  score = EXCLUDED.score,
  upvotes = EXCLUDED.upvotes,
  downvotes = EXCLUDED.downvotes,
  comments = EXCLUDED.comments,
  ingested_at = now();
"""


def _sql_safe(v):
    # Converts pandas NaT/NaN to None so Postgres gets NULL
    if v is None or pd.isna(v):
        return None

    # Convert pandas Timestamp -> python datetime (psycopg2 handles it cleanly)
    if isinstance(v, pd.Timestamp):
        return v.to_pydatetime()

    return v


def upsert_posts(conn_str: str, df) -> int:
    if df.empty:
        return 0

    cols = [
        "post_id", "name", "url", "body", "published", "updated",
        "community_name", "creator_name",
        "score", "upvotes", "downvotes", "comments"
    ]

    # Build tuples in the same order as your INSERT columns
    values = [
        tuple(_sql_safe(row[c]) for c in cols)
        for _, row in df.iterrows()
    ]

    with psycopg2.connect(conn_str) as conn:
        with conn.cursor() as cur:
            execute_values(cur, UPSERT_SQL, values, page_size=500)

    return len(values)


def get_latest_published(conn_str: str, community_name: str):
    """
    Returns the newest 'published' timestamp already stored for this community.
    If table has no rows for that community yet, returns None.
    """
    sql = """
    SELECT MAX(published)
    FROM lemmy_posts
    WHERE community_name = %s;
    """
    with psycopg2.connect(conn_str) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, (community_name,))
            return cur.fetchone()[0]  # datetime or None