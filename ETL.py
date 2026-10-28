import pandas as pd
import sys
from cassandra.cluster import Cluster


def read_csv(file_path):
    df = pd.read_csv(file_path)

    required_columns = {
        "resource",
        "metric_name",
        "collected_at",
        "value"
    }

    if not required_columns.issubset(df.columns):
        missing_columns = required_columns - set(df.columns)
        raise ValueError(f"CSV is missing columns: {missing_columns}")

    resource = df["resource"].tolist()
    metric_name = df["metric_name"].tolist()
    collected_at = df["collected_at"].astype(int).tolist()
    value = df["value"].tolist()

    return resource, metric_name, collected_at, value


def insert_into_cassandra(resource, metric_name, collected_at, value,
                          keyspace, table):

    cluster = Cluster(["127.0.0.1"])
    session = cluster.connect(keyspace)

    query = f"""
        INSERT INTO {table}
        (resource, metric_name, collected_at, value)
        VALUES (?, ?, ?, ?)
    """

    prepared = session.prepare(query)

    for i in range(len(resource)):
        session.execute(prepared, (
            resource[i],
            metric_name[i],
            collected_at[i],
            value[i]
        ))

    cluster.shutdown()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python csv_reader.py <file.csv>")
        sys.exit(1)


