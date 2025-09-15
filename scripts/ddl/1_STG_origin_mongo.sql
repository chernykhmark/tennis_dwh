CREATE TABLE IF NOT EXISTS stg.mongo_matches (
    id SERIAL NOT NULL PRIMARY KEY,
    object_id varchar NOT NULL UNIQUE,
    object_value text NOT NULL,
    update_ts timestamp NOT NULL
);