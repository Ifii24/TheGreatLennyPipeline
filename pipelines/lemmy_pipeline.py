import os
import pandas as pd
from typing import List, Dict, Any

from etls.lemmy_etl import fetch_posts
from etls.postgres_etl import upsert_posts


def _normalize_post_views(post_views: List[Dict[str, Any]]) -> pd.DataFrame:
    rows = []
    for pv in post_views:
        post = pv.get("post", {})
        counts = pv.get("counts", {})
        creator = pv.get("creator", {})
        community = pv.get("community", {})

        rows.append({
            "post_id": post.get("id"),
            "name": post.get("name"),
            "url": post.get("url"),
            "body": post.get("body"),
            "published": post.get("published"),
            "updated": post.get("updated"),
            "community_name": community.get("name"),
            "creator_name": creator.get("name"),
            "score": counts.get("score"),
            "upvotes": counts.get("upvotes"),
            "downvotes": counts.get("downvotes"),
            "comments": counts.get("comments"),
        })

    df = pd.DataFrame(rows)

    if not df.empty:
        df["published"] = pd.to_datetime(df["published"], errors="coerce", utc=True)
        df["updated"] = pd.to_datetime(df["updated"], errors="coerce", utc=True)

    return df


def lemmy_to_postgres_pipeline(community: str, limit: int = 100, sort: str = "New"):
    instance_url = os.environ.get("LEMMY_INSTANCE_URL", "https://lemmy.world")
    pg_conn_str = os.environ["POSTGRES_CONN_STR"]

    # --- NEW: fetch multiple pages until we have up to `limit` posts ---
    all_posts = []
    page = 1

    while len(all_posts) < limit:
        batch = fetch_posts(
            instance_url=instance_url,
            community=community,
            limit=limit,
            sort=sort,
            page=page,
        )

        if not batch:
            break

        all_posts.extend(batch)

        if page >= 10:  # safety guard
            break

        page += 1

    post_views = all_posts[:limit]
    # --- end NEW ---

    df = _normalize_post_views(post_views)
    inserted = upsert_posts(pg_conn_str, df)

    return inserted