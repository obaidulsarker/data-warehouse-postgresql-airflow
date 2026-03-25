#Airflow Setup

### Install docker engine and docker compose

### Create necessary directories
```
mkdir -p /data/airflow
```
### Copy of project contents (inner contents of ETL-PIPELINE directory) and paste to /data/airflow directory

- all dag files must be inside of /data/airflow/dags directory.
- docker-compose.yaml file must be inside of /data/airflow directory.
- .env file must be inside of /data/airflow directory. env file contain BASE DIRECTORY of airflow project.

### Intialize the airflow database
```
docker compose up airflow-init

```

### Start the airflow services
```
docker compose up -d

```
### Check airflow containers status and logs
```
docker ps

docker logs <<CONTAINER-ID>> --tail 100

docker exec -it <<CONTAINER-ID>> /bin/bash

```

### Airflow URL:

http://<<IP-ADDRESS>>:8080

- Default Username and Password
Username: airflow
Password: airflow

## Database User Creations and Permissions: 

- The target connection user must have privileges to CREATE TABLE, ALTER TABLE, INSERT, UPDATE in target schema.

- The source connection user must SELECT on source tables.

- Must allow airflow server IP address in both servers pg_hba.conf file and reload the database services.

## Airflow Connections (set in Airflow UI (Admin → Connections)):

- postgres_source — connection to your operational Postgres database (host, port, schema optional). Set Conn ID name equal to sync_source_conn_id Airflow Variable if you changed it.

- source_conn_id_mohajon
- source_conn_id_oms
- source_conn_id_pegasus

- postgres_target — connection to your data-warehouse Postgres (target).
- target_conn_id_dw

## Airflow Variables (set in Admin → Variables):

- sync_tables_xml_path — path to the XML mapping file (default /opt/airflow/dags/tables_map.xml)

- sync_chunk_size — default 1000 (rows per chunk)

- sync_schedule — a cron or preset like @daily (default @daily)

## Create data model in Target Server (DW server):

- Create a database like as warehouse.
- Create bronze, silver, gold and etl schema.
- Execute "sql/create_etl_stats.sql" into data warehouse database.
- Execute "sql/dw_schema.sql" into data warehouse database.
- Execute "sql/fn_sync_fact_order_stats.sql" into data warehouse database.
- Execute "sql/mv_order_delivery_status.sql" into data warehouse database.

## Map source and target tables:

- list source and target tables in "dags/tables_map.xml" file. You need to maintain proper format of this file.
- Each source table must have auto increment primary key column and updated_at timestamp column.
- updated_at column must be updated during insertion and deletion of records.
- All of bronze layer tables will be created initially the pipeline started execution.
- If any tables in source database changed the schema, it will be synced automatically before data sync process started.
- If any records are get deleted from source database, it will not be deleted in target database.

```
<?xml version="1.0" encoding="UTF-8"?>
<sources>
    <source source_db_name="mohajon" source_conn_id="source_conn_id_mohajon" target_db_name="carrybee_dw" target_conn_id="target_conn_id_dw">
        <table table_no="1" source_schema="public" source_table="branches" target_schema="bronze_mohajon" target_table="branches" pk_column="id" ts_column="updated_at"/>
        <table table_no="2" source_schema="public" source_table="hub_ledgers" target_schema="bronze_mohajon" target_table="hub_ledgers" pk_column="id" ts_column="updated_at"/>
        <table table_no="3" source_schema="public" source_table="hub_payment_invoices" target_schema="bronze_mohajon" target_table="hub_payment_invoices" pk_column="id" ts_column="updated_at"/>
        <table table_no="4" source_schema="public" source_table="hub_payment_transactions" target_schema="bronze_mohajon" target_table="hub_payment_transactions" pk_column="id" ts_column="updated_at"/>
        <table table_no="5" source_schema="public" source_table="hub_payments" target_schema="bronze_mohajon" target_table="hub_payments" pk_column="id" ts_column="updated_at"/>
        <table table_no="6" source_schema="public" source_table="order_invoices" target_schema="bronze_mohajon" target_table="order_invoices" pk_column="id" ts_column="updated_at"/>
        <table table_no="7" source_schema="public" source_table="payment_invoices" target_schema="bronze_mohajon" target_table="payment_invoices" pk_column="id" ts_column="updated_at"/>
        <table table_no="8" source_schema="public" source_table="recoveries" target_schema="bronze_mohajon" target_table="recoveries" pk_column="id" ts_column="updated_at"/>
        <table table_no="9" source_schema="public" source_table="wallet_providers" target_schema="bronze_mohajon" target_table="wallet_providers" pk_column="id" ts_column="updated_at"/>
    </source>
    <source source_db_name="oms" source_conn_id="source_conn_id_oms" target_db_name="carrybee_dw" target_conn_id="target_conn_id_dw">
        <table table_no="1" source_schema="public" source_table="orders" target_schema="bronze_oms" target_table="orders" pk_column="id" ts_column="updated_at"/>
        <table table_no="2" source_schema="public" source_table="order_logs" target_schema="bronze_oms" target_table="order_logs" pk_column="id" ts_column="updated_at"/>
        <table table_no="3" source_schema="public" source_table="hubs" target_schema="bronze_oms" target_table="hubs" pk_column="id" ts_column="updated_at"/>
        <table table_no="4" source_schema="public" source_table="cities" target_schema="bronze_oms" target_table="cities" pk_column="id" ts_column="updated_at"/>
        <table table_no="5" source_schema="public" source_table="zones" target_schema="bronze_oms" target_table="zones" pk_column="id" ts_column="updated_at"/>
        <table table_no="6" source_schema="public" source_table="customers" target_schema="bronze_oms" target_table="customers" pk_column="id" ts_column="updated_at"/>
        <table table_no="7" source_schema="public" source_table="transfer_statuses" target_schema="bronze_oms" target_table="transfer_statuses" pk_column="id" ts_column="updated_at"/>
        <table table_no="8" source_schema="public" source_table="areas" target_schema="bronze_oms" target_table="areas" pk_column="id" ts_column="updated_at"/>
    </source>
    <source source_db_name="pegasus" source_conn_id="source_conn_id_pegasus" target_db_name="carrybee_dw" target_conn_id="target_conn_id_dw">
        <table table_no="1" source_schema="public" source_table="agents" target_schema="bronze_pegasus" target_table="agents" pk_column="id" ts_column="updated_at"/>
        <table table_no="2" source_schema="public" source_table="merchants" target_schema="bronze_pegasus" target_table="merchants" pk_column="id" ts_column="updated_at"/>
        <table table_no="3" source_schema="public" source_table="routes" target_schema="bronze_pegasus" target_table="routes" pk_column="id" ts_column="updated_at"/>
        <table table_no="4" source_schema="public" source_table="clusters" target_schema="bronze_pegasus" target_table="clusters" pk_column="id" ts_column="updated_at"/>
        <table table_no="5" source_schema="public" source_table="regions" target_schema="bronze_pegasus" target_table="regions" pk_column="id" ts_column="updated_at"/>
        <table table_no="6" source_schema="public" source_table="stores" target_schema="bronze_pegasus" target_table="stores" pk_column="id" ts_column="updated_at"/>
        <table table_no="7" source_schema="public" source_table="businesses" target_schema="bronze_pegasus" target_table="businesses" pk_column="id" ts_column="updated_at"/>
    </source>
</sources>
```
- You can add new tables to this map config file.
- You can remove any tables from this file.

## Steps to deploy

- Put sync_tables_dag.py and sync_utils.py in your Airflow DAGs folder.

- Put tables_map.xml in the path referenced by sync_tables_xml_path variable.

- Install required Python packages in the Airflow environment (requirements.txt).


## Notes & caveats

This approach assumes single-column primary keys. If tables have composite PKs, the code needs to be extended to build ON CONFLICT on multiple columns.

Schema sync here is best-effort: it creates missing columns and uses simplified type mapping. It will not attempt risky type conversions — you should validate column types for critical columns manually before the first run.

For very large tables you might want to sync via COPY (dump/load) or use CDC (logical replication / Debezium). This implementation is row-based via last_updated and chunked selects.

If last_updated has low precision or updates are not reliable, consider using id ranges or an incremental marker field.

Error handling: failed table sync writes to etl.etl_stats status=FAILED and includes error snippet.

## Optional improvements (you may ask me to implement)

Support composite primary keys.

Upsert via ctid-less high-performance COPY + staging table swap pattern for very large batch loads.

Pre- and post- validation checks (row counts diff alerts).

Email / Slack notifications when a table fails.

Use TaskGroup or dynamic DAG generation at parse-time to avoid potential runtime dynamic creation restrictions.

Add unit tests & local runner script for dev.

Use CDC (Debezium/logical replication) for near-real-time, low-latency sync.#   d a t a - w a r e h o u s e - p o s t g r e s q l - a i r f l o w 
 
 
