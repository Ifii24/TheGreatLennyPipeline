import requests


def fetch_posts(instance_url: str, community: str, limit: int = 100, sort: str = "New", page: int = 1):
    """
    Fetch post "views" from Lemmy.
    community example: "asklemmy@lemmy.world"
    sort examples: "New", "Hot", "TopDay", "TopWeek", "TopMonth", "TopYear", "TopAll"
    """
    url = f"{instance_url.rstrip('/')}/api/v3/post/list"
    params = {
        "community_name": community,
        "limit": limit,
        "page": page,
        "sort": sort,
    }

    headers = {"User-Agent": "airflow-lemmy-etl/1.0"}

    r = requests.get(url, params=params, headers=headers, timeout=30)
    r.raise_for_status()
    return r.json().get("posts", [])