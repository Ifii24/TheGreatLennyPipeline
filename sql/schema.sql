CREATE TABLE IF NOT EXISTS lemmy_posts (
    post_id BIGINT PRIMARY KEY,
    name TEXT,
    url TEXT,
    body TEXT,
    published TIMESTAMPTZ,
    updated TIMESTAMPTZ,
    community_name TEXT,
    creator_name TEXT,
    score INT,
    upvotes INT,
    downvotes INT,
    comments INT,
    ingested_at TIMESTAMPTZ DEFAULT NOW()
);