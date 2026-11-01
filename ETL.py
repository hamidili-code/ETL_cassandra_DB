import os
import re
import sys
from datetime import datetime, timedelta
import pandas as pd
from cassandra.cluster import Cluster
from cassandra.concurrent import execute_concurrent_with_args

class Validation:
    REQUIRED_COLUMNS = {"resource", "metric_name", "collected_at", "value"}
    DEFAULT_TTL = 365 * 24 * 3600  # one year, in seconds
    IDENTIFIER_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]{0,47}$")
    TIME_FORMAT = "%Y-%m-%d %H:%M"

    @staticmethod
    def validate_identifier(name, kind):
        """CQL identifiers can't be bound as parameters, so whitelist them."""
        if not Validation.IDENTIFIER_RE.match(name):
            raise ValueError(f"Invalid {kind} name: {name!r}")
        return name

    @staticmethod
    def parse_cli_time(text):
        return datetime.strptime(text, Validation.TIME_FORMAT) 

    @staticmethod
    def get_hosts(arg_hosts):
        hosts = arg_hosts or os.environ.get("CASSANDRA_HOSTS", "127.0.0.1")
        return [h.strip() for h in hosts.split(",") if h.strip()]


class ETL:
    @staticmethod
    def read_csv(file_path):
        df = pd.read_csv(file_path)

        missing = Validation.REQUIRED_COLUMNS - set(df.columns)
        if missing:
            raise ValueError(f"CSV is missing columns: {missing}")

        return df[["resource", "metric_name", "collected_at", "value"]]

    @staticmethod
    def transform(df):
        df = df.copy()

        if pd.api.types.is_numeric_dtype(df["collected_at"]):
            ts = pd.to_datetime(df["collected_at"], unit="s", utc=True)
        else:
            ts = pd.to_datetime(df["collected_at"], format=Validation.TIME_FORMAT, utc=True)
        df["collected_at"] = ts.dt.tz_convert(None)

        df["value"] = pd.to_numeric(df["value"], errors="raise").astype(float)

        df["resource"] = df["resource"].astype(str).str.strip()
        df["metric_name"] = df["metric_name"].astype(str).str.strip()

        if df[["resource", "metric_name", "collected_at", "value"]].isna().any().any():
            raise ValueError("CSV contains empty cells in required columns")

        df["time_bucket"] = df["collected_at"].dt.date
        return df

    @staticmethod
    def create_schema(session, table):
        session.execute(f"""
            CREATE TABLE IF NOT EXISTS {table} (
                resource     text,
                metric_name  text,
                time_bucket  date,
                collected_at timestamp,
                value        double,
                PRIMARY KEY ((resource, metric_name, time_bucket), collected_at)
            ) WITH CLUSTERING ORDER BY (collected_at DESC)
        """)
        session.execute(f"""
            CREATE TABLE IF NOT EXISTS {table}_metrics_by_resource (
                resource    text,
                metric_name text,
                PRIMARY KEY ((resource), metric_name)
            )
        """)

    @staticmethod
    def load(session, df, table, ttl):
        data_params = [
            (r.resource, r.metric_name, r.time_bucket, r.collected_at, r.value, ttl)
            for r in df.itertuples(index=False)
        ]
        lookup_params = [
            (res, met, ttl)
            for res, met in df[["resource", "metric_name"]].drop_duplicates().itertuples(index=False)
        ]

        data_stmt = session.prepare(f"""
            INSERT INTO {table}
            (resource, metric_name, time_bucket, collected_at, value)
            VALUES (?, ?, ?, ?, ?)
            USING TTL ?
        """)
        lookup_stmt = session.prepare(f"""
            INSERT INTO {table}_metrics_by_resource (resource, metric_name)
            VALUES (?, ?)
            USING TTL ?
        """)

        execute_concurrent_with_args(session, data_stmt, data_params, concurrency=100)
        execute_concurrent_with_args(session, lookup_stmt, lookup_params, concurrency=100)
        return len(data_params), len(lookup_params)


class CQL:
    @staticmethod
    def q1(session, table, resource, metric, start, end):
        """Values of one metric for one resource in [start, end)."""
        stmt = session.prepare(f"""
            SELECT collected_at, value FROM {table}
            WHERE resource=? AND metric_name=? AND time_bucket=?
              AND collected_at >= ? AND collected_at < ?
        """)
        buckets, day = [], start.date()
        while day <= (end - timedelta(microseconds=1)).date():
            buckets.append(day)
            day += timedelta(days=1)

        params = [(resource, metric, b, start, end) for b in buckets]
        rows = []
        for ok, result in execute_concurrent_with_args(
            session, stmt, params, concurrency=20, raise_on_first_error=False
        ):
            if not ok:
                raise result
            rows.extend((r.collected_at, r.value) for r in result)
        return sorted(rows)

    @staticmethod
    def q2(session, table, resource):
        """All metric names of one resource."""
        stmt = session.prepare(
            f"SELECT metric_name FROM {table}_metrics_by_resource WHERE resource=?"
        )
        return [r.metric_name for r in session.execute(stmt, (resource,))]

if __name__ == "__main__":
    try:
        keyspace = Validation.validate_identifier(args.keyspace, "keyspace")
        table = Validation.validate_identifier(args.table, "table")
 
        cluster = Cluster(Validation.get_hosts(args.hosts))
        try:
            session = cluster.connect(keyspace)
 
            if args.command == "load":
                if args.create_schema:
                    ETL.create_schema(session, table)
                df = ETL.transform(ETL.read_csv(args.csv_file))
                n_rows, n_pairs = ETL.load(session, df, table, args.ttl)
                print(f"Inserted {n_rows} rows into {table} and "
                      f"{n_pairs} (resource, metric) pairs into {table}_metrics_by_resource")
 
            elif args.command == "q1":
                rows = CQL.q1(session, table, args.resource, args.metric,
                          Validation.parse_cli_time(args.start), Validation.parse_cli_time(args.end))
                for ts, val in rows:
                    print(f"{ts:%Y-%m-%d %H:%M}\t{val}")
                print(f"-- {len(rows)} rows")
 
            elif args.command == "q2":
                metrics = CQL.q2(session, table, args.resource)
                print("\n".join(metrics))
                print(f"-- {len(metrics)} metrics")
        finally:
            cluster.shutdown()
    except (ValueError, RuntimeError, OSError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        sys.exit(1)