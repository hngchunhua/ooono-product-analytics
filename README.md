# ooono-product-analytics
AE Interview Case – Chun Hua

# How to run:
1.	Run ingest.py to ingest raw payload from all 3 API endpoints to bronze
2.	Dbt – Silver: Run stg_drives.sql, stg_users.sql, stg_warnings.sql transform the raw payload from bronze to flatten, deduplicated, standardize data format table. Data transformed are tested with few cases in schema.yml
3.	Dbt – Gold: Run fct_drive_health.sql transform the staging dataset to business ready fact table
4.	fct_drive_health table are connected/imported to visualization tool(Power BI)
5.	Data Ingestion: Python
Run ingest.py to retrieve raw data from the 3 CO-DRIVER Events API endpoints (Drives, Warnings and Users) and load it into the Bronze layer in DuckDB.
cd D:\OOONO
python ingest.py
6.	Data Transformation & Validation: dbt
Run dbt build to execute the Silver and Gold transformations and their associated data-quality tests.
cd D:\OOONO\release_health
dbt build
  a)	Silver: stg_drives.sql, stg_warnings.sql and stg_users.sql flatten raw JSON, remove duplicates and standardize data formats. Data quality is validated through tests defined in schema.yml
  b)	Gold: fct_drive_health.sql transforms the cleaned data into an analytics-ready fact table containing drive-level metrics and reconnection indicators
7.	Visualization: Power BI
Export fct_drive_health from DuckDB to CSV and import it into Power BI.

# How to run at OOONO:
1. AWS for ingestion and scheduling: Run the Python ingestion script every three hours. The script would retrieve incremental data from the CO-DRIVER Events API, using ingestion checkpoints to avoid unnecessary full reloads.
2. Snowflake for data storage: Replace the local DuckDB database with Snowflake. The Bronze layer would retain raw API payloads and ingestion metadata, allowing reprocessing and troubleshooting when necessary.
3. dbt for transformations: Run the existing Silver and Gold models against Snowflake, with the necessary SQL adaptations. Silver would handle JSON flattening, deduplication, timestamp standardization and data quality. Gold would provide the drive-level fact table. dbt tests would run after ingestion and transformation.
4. Steep for reporting: Connect Steep directly to the Gold tables in Snowflake. Define shared metrics such as Unique Affected Users and Drives with Reconnection, with breakdowns by platform, app version, OS version and device.

# Ingestion & Refresh Strategy:
1.	For each API endpoints:
  a.	Drives & Warnings: During the initial ingestion, ingest.py retrieves 35 days of historical data based on received_at, the timestamp when the API server received the event. Subsequent runs use ingestion checkpoints to retrieve only records received after the previous checkpoint.
  b.	Users: Each ingestion retrieves a full snapshot of the Users endpoint. The existing Bronze Users table is replaced with the latest snapshot rather than maintaining historical versions.
2.	Ingest.py will run every 3 hours, so do the dbt transformation at Silver & Gold, and data refresh a visualization tool Steep

# Key Decisions:
1.	In <drives>, ‘session_id’ is not unique due to retransmission. In final gold layer fact table, only the session_id with latest received_at is preserve
2.	During dbt silver transformation, only drive session that happened after 28/Sept is preserve, to ensure the dataset has at least 4 complete weeks only to serve the purpose of this take home assignment
3.	Drive session with user_id=unauthenticated are not remove, but cautiously excluded when define metrics that involve unique users count
4.	With assumption no.1, ‘started_at’ & ‘ended_at’ are timestamp are standardize to UTC. Subsequent Calculation of KPI/metrics that involve date/time are based on utc time
5.	<users> are left out on purpose in final gold layer dbt transformation due to fact that <users> table is a snapshot, where the data only show the latest status of the user, i.e. a user can have ‘premium’ plan in yesterday ingestion, and ‘free’ in today ingestion
6.	Incremental ingestion at Drives & Warnings reduces unnecessary API requests and processing time. It also supports the proposed three-hour refresh frequency, allowing Engineering to investigate newly reported connection issues without waiting for a daily or weekly pipeline run.

# Key Assumptions:
1.	In <drives>, ‘country’ represent the country where the drive happen, ‘started_at’ & ‘ended_at’ represent local time when the drive happen
2.	A ‘bad’ drive represent the drive where at least 1 reconnection (connection_count >= 2) happen during the session

# KPI Definitions:
1. Total Unique Users: Number of distinct users who completed at least one drive during the selected period. Excludes unauthenticated users.
2. Unique Affected Users: Number of distinct users who experienced at least one reconnection during any of their drives in the selected period. Each user is counted only once, regardless of how many affected drives they had. Excludes unauthenticated users.
3. Drives with Reconnection: Number of drives that experienced at least one reconnection during the session.
4. Total Reconnections: Total number of reconnection events across all drives. A drive with three reconnections contributes three to this metric.
5. Total Drives: Total number of unique driving sessions recorded during the selected period, including unauthenticated sessions.


