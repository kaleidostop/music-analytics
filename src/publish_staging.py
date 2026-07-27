"""Validate and publish one ClickHouse staging run to the BI table."""

from __future__ import annotations

import argparse
import base64
import os
import sys
import urllib.error
import urllib.parse
import urllib.request


RECORDING_COLUMNS = (
    "recording_mbid",
    "artist",
    "title",
    "album",
    "release_year",
    "duration_seconds",
    "genre",
    "genre_probability",
    "danceability",
    "danceability_probability",
    "mood_acoustic",
    "mood_acoustic_probability",
    "mood_aggressive",
    "mood_aggressive_probability",
    "mood_happy",
    "mood_happy_probability",
    "mood_party",
    "mood_party_probability",
    "mood_relaxed",
    "mood_relaxed_probability",
    "mood_sad",
    "mood_sad_probability",
    "tonality",
    "tonality_probability",
    "voice_instrumental",
    "voice_instrumental_probability",
    "source_file",
    "loaded_at",
)


class ClickHouse:
    def __init__(
        self, host: str, port: int, database: str, user: str, password: str
    ) -> None:
        self.url = f"http://{host}:{port}/"
        self.database = database
        credentials = base64.b64encode(f"{user}:{password}".encode()).decode()
        self.headers = {
            "Authorization": f"Basic {credentials}",
            "Content-Type": "text/plain; charset=utf-8",
        }

    def execute(self, query: str, **parameters: str) -> str:
        url_parameters = {"database": self.database}
        url_parameters.update(
            {f"param_{name}": value for name, value in parameters.items()}
        )
        request = urllib.request.Request(
            f"{self.url}?{urllib.parse.urlencode(url_parameters)}",
            data=query.encode("utf-8"),
            headers=self.headers,
            method="POST",
        )
        with urllib.request.urlopen(request, timeout=60) as response:
            return response.read().decode("utf-8").strip()


def publish(client: ClickHouse, dag_run_id: str) -> int:
    already_published = int(
        client.execute(
            """
            SELECT count()
            FROM published_runs FINAL
            WHERE dag_run_id = {dag_run_id:String}
            """,
            dag_run_id=dag_run_id,
        )
    )
    if already_published:
        print(f"Run {dag_run_id!r} is already published; nothing to do.")
        return 0

    source_rows, unique_mbids = map(
        int,
        client.execute(
            """
            SELECT count(), uniqExact(recording_mbid)
            FROM staging_recordings
            WHERE dag_run_id = {dag_run_id:String}
            FORMAT TSV
            """,
            dag_run_id=dag_run_id,
        ).split("\t"),
    )
    if source_rows == 0:
        raise ValueError(f"No staging rows found for run {dag_run_id!r}")
    if source_rows != unique_mbids:
        raise ValueError(
            f"Run {dag_run_id!r} contains {source_rows} rows but "
            f"{unique_mbids} unique MBIDs"
        )

    columns = ", ".join(RECORDING_COLUMNS)
    client.execute(
        f"""
        INSERT INTO recordings ({columns})
        SELECT {columns}
        FROM staging_recordings
        WHERE dag_run_id = {{dag_run_id:String}}
        """,
        dag_run_id=dag_run_id,
    )
    client.execute(
        """
        INSERT INTO published_runs (dag_run_id, source_rows)
        VALUES ({dag_run_id:String}, {source_rows:UInt64})
        """,
        dag_run_id=dag_run_id,
        source_rows=str(source_rows),
    )

    published_rows = int(
        client.execute(
            """
            SELECT count()
            FROM recordings FINAL
            WHERE recording_mbid IN
            (
                SELECT recording_mbid
                FROM staging_recordings
                WHERE dag_run_id = {dag_run_id:String}
            )
            """,
            dag_run_id=dag_run_id,
        )
    )
    if published_rows != source_rows:
        raise RuntimeError(
            f"Expected {source_rows} published rows, found {published_rows}"
        )

    print(f"Published {published_rows} recordings from run {dag_run_id!r}.")
    return published_rows


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Publish one validated staging run to ClickHouse recordings."
    )
    parser.add_argument("dag_run_id", help="staging run identifier to publish")
    parser.add_argument(
        "--host", default=os.getenv("CLICKHOUSE_HOST", "localhost")
    )
    parser.add_argument(
        "--port",
        type=int,
        default=int(os.getenv("CLICKHOUSE_PORT", "8123")),
    )
    parser.add_argument(
        "--database", default=os.getenv("CLICKHOUSE_DB", "music")
    )
    parser.add_argument(
        "--user", default=os.getenv("CLICKHOUSE_USER", "etl")
    )
    parser.add_argument(
        "--password", default=os.getenv("CLICKHOUSE_PASSWORD", "etl")
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    client = ClickHouse(
        args.host, args.port, args.database, args.user, args.password
    )
    try:
        publish(client, args.dag_run_id)
    except (ValueError, RuntimeError, urllib.error.URLError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
