CREATE TABLE IF NOT EXISTS music.staging_recordings
(
    dag_run_id String,
    recording_mbid UUID,
    artist Nullable(String),
    title Nullable(String),
    album Nullable(String),
    release_year Nullable(UInt16),
    duration_seconds Nullable(Float32),
    genre LowCardinality(Nullable(String)),
    genre_probability Nullable(Float32),
    danceability LowCardinality(Nullable(String)),
    danceability_probability Nullable(Float32),
    mood_acoustic LowCardinality(Nullable(String)),
    mood_acoustic_probability Nullable(Float32),
    mood_aggressive LowCardinality(Nullable(String)),
    mood_aggressive_probability Nullable(Float32),
    mood_happy LowCardinality(Nullable(String)),
    mood_happy_probability Nullable(Float32),
    mood_party LowCardinality(Nullable(String)),
    mood_party_probability Nullable(Float32),
    mood_relaxed LowCardinality(Nullable(String)),
    mood_relaxed_probability Nullable(Float32),
    mood_sad LowCardinality(Nullable(String)),
    mood_sad_probability Nullable(Float32),
    tonality LowCardinality(Nullable(String)),
    tonality_probability Nullable(Float32),
    voice_instrumental LowCardinality(Nullable(String)),
    voice_instrumental_probability Nullable(Float32),
    source_file String,
    loaded_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = MergeTree
ORDER BY (dag_run_id, recording_mbid);

CREATE TABLE IF NOT EXISTS music.recordings
(
    recording_mbid UUID,
    artist Nullable(String),
    title Nullable(String),
    album Nullable(String),
    release_year Nullable(UInt16),
    duration_seconds Nullable(Float32),
    genre LowCardinality(Nullable(String)),
    genre_probability Nullable(Float32),
    danceability LowCardinality(Nullable(String)),
    danceability_probability Nullable(Float32),
    mood_acoustic LowCardinality(Nullable(String)),
    mood_acoustic_probability Nullable(Float32),
    mood_aggressive LowCardinality(Nullable(String)),
    mood_aggressive_probability Nullable(Float32),
    mood_happy LowCardinality(Nullable(String)),
    mood_happy_probability Nullable(Float32),
    mood_party LowCardinality(Nullable(String)),
    mood_party_probability Nullable(Float32),
    mood_relaxed LowCardinality(Nullable(String)),
    mood_relaxed_probability Nullable(Float32),
    mood_sad LowCardinality(Nullable(String)),
    mood_sad_probability Nullable(Float32),
    tonality LowCardinality(Nullable(String)),
    tonality_probability Nullable(Float32),
    voice_instrumental LowCardinality(Nullable(String)),
    voice_instrumental_probability Nullable(Float32),
    source_file String,
    loaded_at DateTime64(3, 'UTC')
)
ENGINE = ReplacingMergeTree(loaded_at)
ORDER BY recording_mbid;

CREATE VIEW IF NOT EXISTS music.v_recording_overview AS
SELECT *
FROM music.recordings FINAL;

CREATE VIEW IF NOT EXISTS music.v_genre_summary AS
SELECT
    genre,
    count() AS recordings_count,
    avg(genre_probability) AS average_genre_probability,
    countIf(danceability = 'danceable') / count() AS danceable_share,
    avg(duration_seconds) AS average_duration_seconds
FROM music.recordings FINAL
GROUP BY genre;
