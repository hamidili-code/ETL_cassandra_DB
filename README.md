Network Monitoring ETL System – Comprehensive Work Report
Revision of the CSV → Apache Cassandra ETL pipeline: problems found, design decisions, and the resulting implementation.
1. Project Overview
This project is an ETL (Extract, Transform, Load) pipeline for network-monitoring measurements. It reads a CSV file of metrics collected from network resources (routers, switches, firewalls, etc.), validates and converts the data in Python, and loads it into Apache Cassandra. The data is stored so that two specific questions can be answered efficiently:
• Q1: What values did a given metric of a given resource take within a given time range?
• Q2: Which metrics exist for a given resource?
The first version of the project was a working prototype but had several design and implementation problems. This report documents those problems, the redesigned data model, and the revised code.
2. Goals
• Read network metrics from CSV and validate the required columns.
• Transform raw CSV values into proper Cassandra types (timestamp and numeric value).
• Model the data around the two queries (Q1 and Q2), with controlled partition size.
• Keep data for one year using Cassandra TTL.
• Write to all tables concurrently and safely.
• Provide a base for dashboards, alerts, and time-series analysis.
3. Architecture
CSV file → Python ETL (Extract, Transform) → Cassandra driver (concurrent writes) → two Cassandra tables
The script runs on the host machine and connects to Cassandra on port 9042 (published from a Docker container). Contact points are no longer hard-coded; they come from configuration or the CASSANDRA_HOSTS environment variable, with 127.0.0.1 as the default.
4. Technologies
Technology
Purpose
Python 3
ETL logic
pandas
CSV loading and transformation
cassandra-driver
Connection, prepared statements, concurrent execution
Apache Cassandra 5.0
Distributed storage
Docker
Runs Cassandra in a container
5. Problems Found in the Original Version
A review of the first version identified the following issues.
#
Problem
Why it matters
1
Q2 could not be answered. The partition key was (resource, metric_name). Listing the metrics of one resource requires filtering on resource alone.
Without ALLOW FILTERING this is impossible, and with it Cassandra scans the whole table. Q2 needs its own table partitioned by resource (intentional redundancy).
2
Inconsistent arguments. The documentation said three arguments (CSV, keyspace, table), but the code accepted exactly two. The usage message named the wrong file (csv_reader.py) and main() described in the documentation did not exist.
The documented command could not run.
3
No Transform step. The code assumed collected_at was already a Unix timestamp (astype(int)), while the project CSV contains times like 2018-12-16 22:25.
The original line fails on the real project data.
4
Wrong column types. value was stored as text and collected_at as bigint; the value was sent as a number to a text column.
A prepared statement raises a type error; text is also useless for time-series analysis.
5
No time bucket. Partitioning only by resource and metric makes one partition per series grow without limit.
At 5-minute resolution, one year is about 105,000 rows in a single partition.
6
No one-year retention (TTL).
Data would be kept forever.
7
Minor issues: writes only to one table; sequential, synchronous inserts; cluster.shutdown() not in a finally block; keyspace/table names placed directly into an f-string; hard-coded 127.0.0.1.
Poor throughput, resource leaks, injection risk, and inflexible deployment.
Positive points kept from the original: column validation, use of prepared statements, the (resource, metric_name) idea for Q1, and honest documentation of limitations.
6. Data Model
The model was designed query-first: each query's WHERE clause was written down, then a table was designed for it.
6.1 Q1 – values of a metric in a time range
SELECT collected_at, value FROM metrics
WHERE resource = ? AND metric_name = ? AND time_bucket = ?
  AND collected_at >= ? AND collected_at < ?;
6.2 Q2 – metrics of a resource
SELECT metric_name FROM metrics_metrics_by_resource
WHERE resource = ?;
6.3 Schema
CREATE KEYSPACE IF NOT EXISTS network_monitoring
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE IF NOT EXISTS network_monitoring.metrics (
  resource     text,
  metric_name  text,
  time_bucket  date,
  collected_at timestamp,
  value        double,
  PRIMARY KEY ((resource, metric_name, time_bucket), collected_at)
) WITH CLUSTERING ORDER BY (collected_at DESC);

CREATE TABLE IF NOT EXISTS network_monitoring.metrics_metrics_by_resource (
  resource    text,
  metric_name text,
  PRIMARY KEY ((resource), metric_name)
);
The second table's name is the configured table name plus the suffix _metrics_by_resource.
6.4 Design decisions
• Time bucket = one day. With 5-minute data, a partition holds about 288 rows, so partitions stay small and never grow unbounded. A one-week Q1 range touches at most 8 partitions, which are read concurrently and merged.
• timestamp and double. Time is a real timestamp (stored in UTC) and the value is numeric, which is required for time-series analysis and aggregation.
• Newest first. Clustering order is descending by collected_at, so recent data is read first.
• Denormalization. Q2 has its own table partitioned by resource. This duplication is deliberate: Cassandra has no joins, so each query is served by a table shaped for it.
• Retention. Every row is written with USING TTL, default 365 × 24 × 3600 = 31,536,000 seconds (one year).
7. CSV Data Format
Required headers (any order): resource, metric_name, collected_at, value.
Column
Meaning
Example
resource
Monitored device
Router-01
metric_name
Measurement name
CPU Usage
collected_at
Time, as YYYY-MM-DD HH:MM (UTC) or Unix seconds
2018-12-16 22:25
value
Numeric metric value
72
Example:
resource,metric_name,collected_at,value
Network,Packets Dropped,2018-12-16 22:25,1532345
Router-01,CPU Usage,2018-12-16 22:30,72
Router-01,Memory Usage,2018-12-16 22:35,64
8. Code Structure
The code in ETL.py is organized into three classes.
8.1 Validation
Holds constants and validation helpers (all static):
• Constants: REQUIRED_COLUMNS, DEFAULT_TTL, IDENTIFIER_RE, TIME_FORMAT (%Y-%m-%d %H:%M).
• validate_identifier(name, kind): whitelists keyspace and table names with a regular expression. CQL identifiers cannot be bound as parameters, so this prevents injection through names.
• parse_cli_time(text): parses a user-supplied time string.
• get_hosts(arg_hosts): returns the contact points from the argument, the CASSANDRA_HOSTS environment variable, or 127.0.0.1.
8.2 ETL
• read_csv(file_path) – Extract. Loads the CSV and raises an error naming any missing required columns.
• transform(df) – Transform. Converts collected_at to a UTC timestamp (accepting both the date-time string format and numeric Unix seconds), converts value to float (invalid values raise an error instead of being silently hidden), strips whitespace in text columns, rejects empty cells, and derives the daily time_bucket.
• create_schema(session, table) – creates both tables if they do not exist.
• load(session, df, table, ttl) – Load. Prepares both INSERT ... USING TTL ? statements and writes all rows to the main table and the distinct (resource, metric_name) pairs to the lookup table, using execute_concurrent_with_args with concurrency 100.
8.3 CQL
• q1(session, table, resource, metric, start, end) – computes the daily buckets covered by [start, end), queries them concurrently, and returns the rows sorted by time.
• q2(session, table, resource) – returns all metric names of a resource from the lookup table.
8.4 Entry point
The separate main() function, build_parser() (and the argparse dependency), day_bucket() and the helper _run_concurrent() were removed. The control flow now lives directly under if __name__ == "__main__":. It validates the keyspace and table names, connects to Cassandra, runs the requested command (load, q1 or q2), and always calls cluster.shutdown() in a finally block. Expected errors (ValueError, RuntimeError, OSError) are printed to standard error with exit code 1.
9. Configuration Parameters
Command-line parsing was removed on purpose. The parameters that previously came from the parser are to be supplied from a conf.yaml file:
Parameter
Used by
Notes
hosts
all
Default: $CASSANDRA_HOSTS or 127.0.0.1
command
all
load, q1 or q2
keyspace
all
Validated against the identifier pattern
table
all
Validated against the identifier pattern
csv_file
load
Path to the input CSV
ttl
load
Seconds; default 31,536,000
create_schema
load
true / false
resource
q1, q2
Resource name
metric
q1
Metric name
start
q1
YYYY-MM-DD HH:MM, UTC, inclusive
end
q1
YYYY-MM-DD HH:MM, UTC, exclusive
10. Setup and Operation
10.1 Install dependencies (Ubuntu/Debian)
sudo apt update
sudo apt install python3 python3-venv python3-pip
cd /home/hamidi
python3 -m venv venv
source venv/bin/activate
python -m pip install pandas cassandra-driver
For the configuration file, a YAML parser such as PyYAML will also be needed.
10.2 Start Cassandra with Docker
docker run --name cassandra-node -p 9042:9042 -d cassandra:5.0
docker ps
docker logs cassandra-node
docker exec -it cassandra-node cqlsh
If the container already exists, start it with docker start cassandra-node. Use persistent storage for important data.
10.3 Create keyspace and tables
Run the statements in section 6.3 in cqlsh, or enable the create_schema option so that the script creates the tables (the keyspace itself must already exist, because the script connects to it).
10.4 Verify the data
SELECT * FROM network_monitoring.metrics LIMIT 20;
SELECT metric_name FROM network_monitoring.metrics_metrics_by_resource WHERE resource = 'Router-01';
SELECT collected_at, value FROM network_monitoring.metrics
WHERE resource = 'Router-01' AND metric_name = 'CPU Usage'
  AND time_bucket = '2018-12-16'
  AND collected_at >= '2018-12-16 00:00' AND collected_at < '2018-12-17 00:00';
Compare the results with the input CSV. Row counts reported by the loader should match the number of CSV rows and the number of distinct (resource, metric_name) pairs.
11. Operational Checklist
• Cassandra container is running and port 9042 is published.
• Keyspace and both tables exist.
• The Python environment has pandas and cassandra-driver installed.
• CSV headers include resource, metric_name, collected_at, value.
• collected_at is YYYY-MM-DD HH:MM (UTC) or Unix seconds; value is numeric.
• Configuration provides the command, keyspace, table and the parameters for that command.
• SELECT queries confirm the stored records.
12. Known Limitations and Notes
• Not yet tested against a live cluster. The code compiles, but the Q1 and Q2 outputs against real data still have to be collected and added to this report.
• Configuration is pending. args in the entry block is a placeholder until conf.yaml loading is implemented, so the script cannot run end-to-end yet.
• Write errors are not aggregated. After _run_concurrent was removed, load calls execute_concurrent_with_args directly; failed individual inserts are not collected and reported.
• TTL counts from insertion time, not from collected_at. Historical data loaded today is retained for one year from loading. Old datasets (for example from 2018) therefore live one year from the load, not from the measurement date.
• The lookup table's TTL is refreshed each time a pair is loaded again.
• Timestamps are interpreted as UTC.
• Cassandra is configured with SimpleStrategy and replication factor 1, suitable for development only.
13. Future Improvements
• Load parameters from conf.yaml and complete end-to-end runs.
• Collect and report failed writes, with retries.
• Chunked CSV processing for large files.
• Optional TTL computed from collected_at.
• Logging and ingestion statistics.
• Automated tests for missing columns, invalid timestamps, empty files, repeated measurements and bucket boundaries.
• Production planning: authentication, network security, NetworkTopologyStrategy replication, backups.
14. Appendix – Vim Quick Reference
Action
Keys
Open a file
vim ETL.py
Insert mode
i
Save
Esc, :w, Enter
Save and quit
Esc, :wq, Enter
Run current file
Esc, :!python3 %
Check syntax
python -m py_compile ETL.py
15. Summary
The pipeline was redesigned from a single-table prototype into a query-driven Cassandra model: one table for time-range reads (Q1) bucketed by day, and one lookup table for listing a resource's metrics (Q2). The code now transforms real project data (date-time strings and numeric values) into proper Cassandra types, applies a one-year TTL, writes to both tables concurrently, validates identifiers, and always closes the cluster connection. Remaining work is the configuration file, live-cluster testing, and documenting the actual query results.
