CREATE TABLE IF NOT EXISTS music.staging_recordings
(
    load_id String,
    recording_mbid String,
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
ORDER BY (load_id, recording_mbid);

CREATE TABLE IF NOT EXISTS music.recordings
(
    recording_mbid String,
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

CREATE TABLE IF NOT EXISTS music.published_loads
(
    load_id String,
    source_rows UInt64,
    published_at DateTime64(3, 'UTC') DEFAULT now64(3)
)
ENGINE = ReplacingMergeTree(published_at)
ORDER BY load_id;

CREATE VIEW IF NOT EXISTS music.v_recording_overview AS
SELECT *
FROM music.recordings FINAL;

CREATE VIEW IF NOT EXISTS music.v_genre_summary AS
SELECT
    coalesce(genre, 'unknown') AS genre,
    count() AS recordings_count,
    avg(genre_probability) AS average_genre_probability,
    countIf(genre_probability < 0.5) / count() AS low_confidence_share,
    countIf(
        artist IS NULL OR empty(artist)
        OR title IS NULL OR empty(title)
        OR release_year IS NULL
    ) / count() AS incomplete_metadata_share,
    countIf(danceability = 'danceable') / count() AS danceable_share,
    avg(duration_seconds) AS average_duration_seconds
FROM music.recordings FINAL
GROUP BY genre;

CREATE VIEW IF NOT EXISTS music.v_data_quality_summary AS
SELECT
    recordings_count,
    missing_artist_count,
    missing_title_count,
    missing_year_count,
    missing_genre_count,
    low_confidence_count,
    if(recordings_count = 0, 0, missing_artist_count / recordings_count) AS missing_artist_share,
    if(recordings_count = 0, 0, missing_title_count / recordings_count) AS missing_title_share,
    if(recordings_count = 0, 0, missing_year_count / recordings_count) AS missing_year_share,
    if(recordings_count = 0, 0, missing_genre_count / recordings_count) AS missing_genre_share,
    if(recordings_count = 0, 0, low_confidence_count / recordings_count) AS low_confidence_share
FROM
(
    SELECT
        count() AS recordings_count,
        countIf(artist IS NULL OR empty(artist)) AS missing_artist_count,
        countIf(title IS NULL OR empty(title)) AS missing_title_count,
        countIf(release_year IS NULL) AS missing_year_count,
        countIf(genre IS NULL OR empty(genre)) AS missing_genre_count,
        countIf(genre_probability < 0.5) AS low_confidence_count
    FROM music.recordings FINAL
);
