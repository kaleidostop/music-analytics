"""Read and flatten AcousticBrainz high-level JSON with PySpark."""

from __future__ import annotations

import argparse
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    FloatType,
    StringType,
    StructField,
    StructType,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_INPUT = PROJECT_ROOT / "data" / "raw" / "schema-sample" / "highlevel"
UUID_PATTERN = (
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)
FILENAME_MBID_PATTERN = (
    r"([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-"
    r"[0-9a-fA-F]{4}-[0-9a-fA-F]{12})-\d+\.json$"
)


def classifier_schema() -> StructType:
    """Schema of the classifier fields used by the project."""
    return StructType(
        [
            StructField("value", StringType(), True),
            StructField("probability", FloatType(), True),
        ]
    )


def acousticbrainz_highlevel_schema() -> StructType:
    """Explicit subset of the real AcousticBrainz high-level JSON schema."""
    tags = StructType(
        [
            StructField("album", ArrayType(StringType()), True),
            StructField("artist", ArrayType(StringType()), True),
            StructField("date", ArrayType(StringType()), True),
            StructField("musicbrainz_recordingid", ArrayType(StringType()), True),
            StructField("title", ArrayType(StringType()), True),
        ]
    )
    audio_properties = StructType(
        [StructField("length", DoubleType(), True)]
    )
    highlevel = StructType(
        [
            StructField("danceability", classifier_schema(), True),
            StructField("genre_dortmund", classifier_schema(), True),
            StructField("mood_acoustic", classifier_schema(), True),
            StructField("mood_aggressive", classifier_schema(), True),
            StructField("mood_happy", classifier_schema(), True),
            StructField("mood_party", classifier_schema(), True),
            StructField("mood_relaxed", classifier_schema(), True),
            StructField("mood_sad", classifier_schema(), True),
            StructField("tonal_atonal", classifier_schema(), True),
            StructField("voice_instrumental", classifier_schema(), True),
        ]
    )
    metadata = StructType(
        [
            StructField("audio_properties", audio_properties, True),
            StructField("tags", tags, True),
        ]
    )
    return StructType(
        [
            StructField("highlevel", highlevel, True),
            StructField("metadata", metadata, True),
        ]
    )


def first_tag(name: str):
    return F.element_at(F.col(f"metadata.tags.{name}"), 1)


def valid_probability(path: str):
    value = F.col(path).cast("float")
    return F.when(value.between(0.0, 1.0), value)


def transform_highlevel(raw: DataFrame) -> DataFrame:
    """Flatten fields selected for ClickHouse and remove invalid MBIDs."""
    mbid_from_filename = F.regexp_extract(
        F.input_file_name(), FILENAME_MBID_PATTERN, 1
    )
    recording_mbid = F.coalesce(first_tag("musicbrainz_recordingid"), mbid_from_filename)
    date = first_tag("date")

    transformed = raw.select(
        F.lower(recording_mbid).alias("recording_mbid"),
        first_tag("artist").alias("artist"),
        first_tag("title").alias("title"),
        first_tag("album").alias("album"),
        F.regexp_extract(date, r"^(\d{4})", 1).cast("int").alias("release_year"),
        F.col("metadata.audio_properties.length")
        .cast("float")
        .alias("duration_seconds"),
        F.col("highlevel.genre_dortmund.value").alias("genre"),
        valid_probability("highlevel.genre_dortmund.probability").alias(
            "genre_probability"
        ),
        F.col("highlevel.danceability.value").alias("danceability"),
        valid_probability("highlevel.danceability.probability").alias(
            "danceability_probability"
        ),
        F.col("highlevel.mood_acoustic.value").alias("mood_acoustic"),
        valid_probability("highlevel.mood_acoustic.probability").alias(
            "mood_acoustic_probability"
        ),
        F.col("highlevel.mood_aggressive.value").alias("mood_aggressive"),
        valid_probability("highlevel.mood_aggressive.probability").alias(
            "mood_aggressive_probability"
        ),
        F.col("highlevel.mood_happy.value").alias("mood_happy"),
        valid_probability("highlevel.mood_happy.probability").alias(
            "mood_happy_probability"
        ),
        F.col("highlevel.mood_party.value").alias("mood_party"),
        valid_probability("highlevel.mood_party.probability").alias(
            "mood_party_probability"
        ),
        F.col("highlevel.mood_relaxed.value").alias("mood_relaxed"),
        valid_probability("highlevel.mood_relaxed.probability").alias(
            "mood_relaxed_probability"
        ),
        F.col("highlevel.mood_sad.value").alias("mood_sad"),
        valid_probability("highlevel.mood_sad.probability").alias(
            "mood_sad_probability"
        ),
        F.col("highlevel.tonal_atonal.value").alias("tonality"),
        valid_probability("highlevel.tonal_atonal.probability").alias(
            "tonality_probability"
        ),
        F.col("highlevel.voice_instrumental.value").alias("voice_instrumental"),
        valid_probability("highlevel.voice_instrumental.probability").alias(
            "voice_instrumental_probability"
        ),
        F.input_file_name().alias("source_file"),
    )

    return (
        transformed.filter(F.col("recording_mbid").rlike(UUID_PATTERN))
        .dropDuplicates(["recording_mbid"])
    )


def read_highlevel(spark: SparkSession, input_path: Path) -> DataFrame:
    files = sorted(str(path.resolve()) for path in input_path.rglob("*.json"))
    if not files:
        raise ValueError(f"No JSON files found under {input_path}")
    return spark.read.schema(acousticbrainz_highlevel_schema()).json(files)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Flatten AcousticBrainz high-level JSON with local PySpark."
    )
    parser.add_argument(
        "--input",
        type=Path,
        default=DEFAULT_INPUT,
        help="directory containing extracted JSON",
    )
    parser.add_argument(
        "--master",
        default="local[2]",
        help="Spark master URL (default: local[2])",
    )
    parser.add_argument(
        "--show",
        type=int,
        default=10,
        help="number of transformed rows to display",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    if not args.input.is_dir():
        print(f"Input directory does not exist: {args.input}")
        return 2
    if args.show < 0:
        print("--show cannot be negative")
        return 2

    spark = (
        SparkSession.builder.appName("acousticbrainz-highlevel-transform")
        .master(args.master)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    try:
        raw = read_highlevel(spark, args.input)
        transformed = transform_highlevel(raw).cache()

        print(f"Raw documents: {raw.count()}")
        print(f"Valid unique recordings: {transformed.count()}")
        transformed.printSchema()
        if args.show:
            transformed.show(args.show, truncate=40, vertical=True)
    finally:
        spark.stop()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
