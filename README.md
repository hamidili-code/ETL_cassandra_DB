# ETL_cassandra_DB
This project is a basic ETL (Extract, Transform, Load) pipeline for network-monitoring measurements. It reads a CSV file, checks required columns, prepares values in Python, and inserts records into Apache Cassandra. It is a foundation for monitoring routers, switches, firewalls, and other network resources.


Network Monitoring ETL System
Project Documentation — A to Z
1. Project Overview
This project is a basic ETL (Extract, Transform, Load) pipeline for network-monitoring measurements. It reads a CSV file, checks required columns, prepares values in Python, and inserts records into Apache Cassandra. It is a foundation for monitoring routers, switches, firewalls, and other network resources.
2. Goals
•	Read network metrics from CSV.
•	Check required column names.
•	Convert collection times to Unix timestamps.
•	Insert records into Cassandra with prepared statements.
•	Provide a base for dashboards, alerts, and analysis.
3. Architecture
CSV file → Python ETL → Cassandra driver → Cassandra table
The Python script runs on the host machine and connects to Cassandra at 127.0.0.1:9042 when Docker publishes port 9042 to the host.
4. Technologies
Technology	Purpose
Python 3	ETL logic and command-line execution
pandas	CSV loading and column-based access
cassandra-driver	Connects to Cassandra and runs CQL
Apache Cassandra 5.0	Distributed data storage
Docker	Runs Cassandra in a container
5. CSV Data Format
The CSV must include the following headers. They can appear in any order.
Column	Meaning	Example
resource	Device or monitored resource	Router-01
metric_name	Measurement name	CPU Usage
collected_at	Unix timestamp in seconds	1544999100
value	Metric value stored as text	72
The current implementation expects collected_at to be convertible to an integer and converts value to text before insertion.
6. Example CSV
resource,metric_name,collected_at,value
Network,Packets Dropped,1544999100,1532345
Network,Packets Received,1544999160,1532450
Router-01,CPU Usage,1544999220,72
Router-01,Memory Usage,1544999280,64
7. Cassandra Schema
Create the keyspace in cqlsh:
CREATE KEYSPACE IF NOT EXISTS network_monitoring
WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': 1
};
Create the table:
CREATE TABLE IF NOT EXISTS network_monitoring.metrics (
  resource text,
  metric_name text,
  collected_at bigint,
  value text,
  PRIMARY KEY ((resource, metric_name), collected_at)
);
This primary key groups records by resource and metric, with collection time as the clustering column. For production, design partition keys and time buckets around expected queries and data volume.
8. ETL Components
•	read_csv(file_path): loads the CSV, checks required headers, and returns four column lists.
•	insert_into_cassandra(...): connects to Cassandra, prepares an INSERT statement, and inserts each record.
•	main(): reads command-line arguments and coordinates reading and inserting.
•	Prepared statements bind data values separately from the CQL statement.
9. Install Dependencies
On Ubuntu/Debian:
sudo apt update
sudo apt install python3 python3-venv python3-pip
Create and activate a virtual environment in the project directory:
cd /home/hamidi
python3 -m venv venv
source venv/bin/activate
Install project dependencies:
python -m pip install pandas cassandra-driver
Check the active interpreter and pandas installation:
which python
python -c "import pandas; print(pandas.__version__)"
10. Start Cassandra with Docker
If creating a new container and the image is available:
docker run --name cassandra-node -p 9042:9042 -d cassandra:5.0
Check container status and logs:
docker ps
docker logs cassandra-node
Open the Cassandra query shell:
docker exec -it cassandra-node cqlsh
If the container already exists, start it with docker start cassandra-node. Keep persistent storage for important data.
11. Run the ETL Script
Activate the virtual environment, make sure Cassandra is running, then run:
python ETL.py network_metrics.csv network_monitoring metrics
Arguments are the CSV path, keyspace, and table. Use an absolute CSV path if the file is in another directory.
12. Verify the Data
In cqlsh, run:
SELECT * FROM network_monitoring.metrics;
Compare the returned records with the input CSV.
13. Vim Quick Reference
•	Open a file: vim ETL.py
•	Enter Insert mode: press i.
•	Save: press Esc, type :w, press Enter.
•	Save and quit: press Esc, type :wq, press Enter.
•	Run current file from Vim: press Esc and type :!python3 %.
•	Check Python syntax: python -m py_compile ETL.py.
14. Operational Checklist
•	Cassandra container is running and port 9042 is published.
•	Keyspace and table exist.
•	The active Python environment has pandas and cassandra-driver installed.
•	CSV headers include resource, metric_name, collected_at, and value.
•	collected_at contains integer Unix timestamps in seconds.
•	Data types match the Cassandra schema.
•	The run command includes the CSV path, keyspace, and table.
•	A SELECT query confirms the stored records.
15. Future Improvements
•	Use chunked CSV processing for large files.
•	Use controlled concurrent or asynchronous inserts to improve throughput.
•	Validate keyspace and table identifiers before inserting them into CQL.
•	Add logging, retry handling, and ingestion statistics.
•	Design time buckets and partition keys based on query patterns.
•	Add tests for missing columns, invalid timestamps, empty files, and repeated measurements.
•	Plan authentication, network security, replication, backups, and retention for production.
16. Summary
This project demonstrates a basic ETL workflow: extract monitoring data from CSV, validate and prepare it in Python, and load it into Cassandra with prepared statements. It is a learning and prototyping foundation. Production readiness depends on data modeling, ingestion performance, resilience, security, and operational testing.

