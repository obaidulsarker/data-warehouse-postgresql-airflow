"""
sync_tables_dag.py
Airflow DAG for PostgreSQL → PostgreSQL incremental sync.
Uses automatic CREATE TABLE generation (Option A).
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
import pendulum
from datetime import datetime, timedelta
import logging
import traceback
import requests

from sync_utils import (
    parse_tables_map,
    get_pg_hook,
    get_source_max_updated,
    get_source_row_count,
    get_source_row_count_by_timestamp,
    get_last_sync_info,
    upsert_etl_stats,
    fetch_changed_rows,
    upsert_rows_to_target,
    ensure_target_table_exists_and_add_missing_columns,
    get_source_columns_with_details,
    get_primary_key_columns,
    get_target_row_count,
    ensure_target_schema_exists,
    execute_function,
    get_last_sync_silver_fact_table,
    refresh_materialized_view
)

log = logging.getLogger(__name__)

DAG_ID = "operation_to_data_warehosue_dag"
#XML_PATH = Variable.get("sync_xml_path", default_var="/opt/airflow/dags/tables_map.xml")
SCHEDULE = Variable.get("sync_schedule", default_var="@hourly")
CHUNK_SIZE = int(Variable.get("sync_chunk_size", default_var=1000))

XML_PATH = "/opt/airflow/dags/tables_map.xml"

#start_date = pendulum.now().subtract(days=1)

overlap_minutes = 5

# Notfication
def discord_failure_callback(context):
        webhook_url = Variable.get("discord_webhook_url")


        dag_id = context.get("dag").dag_id
        task_id = context.get("task_instance").task_id
        execution_date = context.get("execution_date")
        log_url = context.get("task_instance").log_url
        exception = context.get("exception")


        message = {
            "username": "Airflow Alert 🚨",
            "embeds": [
            {
                "title": "❌ Airflow DAG Failed",
                "color": 15158332, # red
                "fields": [
                {"name": "DAG", "value": dag_id, "inline": False},
                {"name": "Task", "value": task_id, "inline": False},
                {"name": "Execution Time", "value": str(execution_date), "inline": False},
                {"name": "Error", "value": f"```{str(exception)[:1000]}```", "inline": False},
                {"name": "Logs", "value": log_url, "inline": False},
                ],
            }
            ],
        }


        requests.post(webhook_url, json=message, timeout=10)

default_args = {
    "owner": "root",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "depends_on_past": False
}

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    start_date=pendulum.now().subtract(days=1),
    schedule=SCHEDULE,
    catchup=False,
    max_active_runs=1,
    tags=["pg-sync", "etl"],
    on_failure_callback=discord_failure_callback,
) as dag:

    #refresh_mv_delivery_aging_last_7days
    def refresh_mv_delivery_aging_last_7days(**context):
        try:
            target_hook = get_pg_hook("target_conn_id_dw")
            
            log.info(f"=== Sync Start: mv_delivery_aging_last_7days ===")

            sql = "REFRESH MATERIALIZED VIEW gold.mv_delivery_aging_last_7days;"

            fn_exec_status = refresh_materialized_view(pg_hook=target_hook, mv_refresh_sql=sql)

            if fn_exec_status is None:
                raise Exception(f"Error is orccured: {sql} ")

            log.info(f"=== Sync End: mv_delivery_aging_last_7days ===")

        
        except Exception as e:
            log.error(f"ERROR: {e}", exc_info=True)
            raise

    #refresh_mv_delivery_status_last_7days
    def refresh_mv_delivery_status_last_7days(**context):
        try:
            target_hook = get_pg_hook("target_conn_id_dw")
            
            log.info(f"=== Sync Start: mv_delivery_status_last_7days ===")

            sql = "REFRESH MATERIALIZED VIEW gold.mv_delivery_status_last_7days;"

            fn_exec_status = refresh_materialized_view(pg_hook=target_hook, mv_refresh_sql=sql)

            if fn_exec_status is None:
                raise Exception(f"Error is orccured: {sql} ")

            log.info(f"=== Sync End: mv_delivery_status_last_7days ===")

        
        except Exception as e:
            log.error(f"ERROR: {e}", exc_info=True)
            raise

    # Refresh  mv_daily_orders_summary
    def refresh_mv_orders_daily_summary(**context):
        try:
            target_hook = get_pg_hook("target_conn_id_dw")
            
            log.info(f"=== Sync Start: mv_daily_orders_summary ===")

            sql = "REFRESH MATERIALIZED VIEW gold.mv_daily_orders_summary;"

            fn_exec_status = refresh_materialized_view(pg_hook=target_hook, mv_refresh_sql=sql)

            if fn_exec_status is None:
                raise Exception(f"Error is orccured: {sql} ")

            log.info(f"=== Sync End: mv_daily_orders_summary ===")

        
        except Exception as e:
            log.error(f"ERROR: {e}", exc_info=True)
            raise
    
    # Refresh  refresh_mv_agent_report
    def refresh_mv_agent_report(**context):
        try:
            target_hook = get_pg_hook("target_conn_id_dw")
            
            log.info(f"=== Sync Start: refresh_mv_agent_report ===")

            sql = "REFRESH MATERIALIZED VIEW gold.mv_agent_report;"

            fn_exec_status = refresh_materialized_view(pg_hook=target_hook, mv_refresh_sql=sql)

            if fn_exec_status is None:
                raise Exception(f"Error is orccured: {sql} ")

            log.info(f"=== Sync End: refresh_mv_agent_report ===")

        
        except Exception as e:
            log.error(f"ERROR: {e}", exc_info=True)
            raise
    
    # Refresh  mv_daily_order_invoice
    def refresh_mv_daily_order_invoice(**context):
        try:
            target_hook = get_pg_hook("target_conn_id_dw")
            
            log.info(f"=== Sync Start: refresh_mv_daily_order_invoice ===")

            sql = "REFRESH MATERIALIZED VIEW gold.mv_daily_order_invoice;"

            fn_exec_status = refresh_materialized_view(pg_hook=target_hook, mv_refresh_sql=sql)

            if fn_exec_status is None:
                raise Exception(f"Error is orccured: {sql} ")

            log.info(f"=== Sync End: refresh_mv_daily_order_invoice ===")

        
        except Exception as e:
            log.error(f"ERROR: {e}", exc_info=True)
            raise
    
    # Refresh  mv_daily_order_invoice
    def refresh_mv_daily_payment_invoice(**context):
        try:
            target_hook = get_pg_hook("target_conn_id_dw")
            
            log.info(f"=== Sync Start: refresh_mv_daily_payment_invoice ===")

            sql = "REFRESH MATERIALIZED VIEW gold.mv_daily_payment_invoice;"

            fn_exec_status = refresh_materialized_view(pg_hook=target_hook, mv_refresh_sql=sql)

            if fn_exec_status is None:
                raise Exception(f"Error is orccured: {sql} ")

            log.info(f"=== Sync End: refresh_mv_daily_payment_invoice ===")

        
        except Exception as e:
            log.error(f"ERROR: {e}", exc_info=True)
            raise

    # refresh_mv_order_global_report
    def refresh_mv_order_global_report(**context):
        try:
            target_hook = get_pg_hook("target_conn_id_dw")
            
            log.info(f"=== Sync Start: refresh_mv_order_global_report ===")

            sql = "REFRESH MATERIALIZED VIEW gold.mv_order_global_report;"

            fn_exec_status = refresh_materialized_view(pg_hook=target_hook, mv_refresh_sql=sql)

            if fn_exec_status is None:
                raise Exception(f"Error is orccured: {sql} ")

            log.info(f"=== Sync End: refresh_mv_order_global_report ===")

        
        except Exception as e:
            log.error(f"ERROR: {e}", exc_info=True)
            raise

    # Sync fact_daily_orders_summary Table
    def sync_fact_daily_orders_summary():

        try:
            target_hook = get_pg_hook("target_conn_id_dw")

            log.info(f"=== Sync Start: fact_daily_orders_summary Fact ===")
            
            # Get Last Sync ts
            last = get_last_sync_silver_fact_table(pg_hook=target_hook, target_schema="silver", target_table="fact_daily_orders_summary")
            last_synced_at = last["last_synced_at"]
            
            sql = f"""SELECT silver.fn_sync_fact_daily_orders_summary('{last_synced_at}')"""

            fn_exec_status = execute_function(pg_hook=target_hook, function_call_sql=sql)

            if fn_exec_status == 0:
                raise Exception(f"Error is orccured: {sql} ")
            
            # else:
            #     # Update last_sync_at in etl water mark table
            #     last_synced_date = datetime.utcnow() - timedelta(minutes=overlap_minutes)
            #     update_last_sync_date = update_last_sync_silver_fact_table(pg_hook=target_hook, target_schema="silver", target_table="fact_orders_stats", last_synced_at=last_synced_date)

            log.info(f"=== Sync End: fact_daily_orders_summary Fact ===")
        
        except Exception as e:
            log.error(f"ERROR: {e}", exc_info=True)
            raise
    
    # Sync fact_order_invoice Table
    def sync_fact_order_invoice():

        try:
            target_hook = get_pg_hook("target_conn_id_dw")

            log.info(f"=== Sync Start: fact_order_invoice Fact ===")
            
            # Get Last Sync ts
            last = get_last_sync_silver_fact_table(pg_hook=target_hook, target_schema="silver", target_table="fact_order_invoice")
            last_synced_at = last["last_synced_at"]
            
            sql = f"""SELECT silver.fn_sync_fact_order_invoice('{last_synced_at}')"""

            fn_exec_status = execute_function(pg_hook=target_hook, function_call_sql=sql)

            if fn_exec_status == 0:
                raise Exception(f"Error is orccured: {sql} ")
            
            # else:
            #     # Update last_sync_at in etl water mark table
            #     last_synced_date = datetime.utcnow() - timedelta(minutes=overlap_minutes)
            #     update_last_sync_date = update_last_sync_silver_fact_table(pg_hook=target_hook, target_schema="silver", target_table="fact_orders_stats", last_synced_at=last_synced_date)

            log.info(f"=== Sync End: fact_order_invoice Fact ===")
        
        except Exception as e:
            log.error(f"ERROR: {e}", exc_info=True)
            raise

    # Sync fact_payment_invoice Table
    def sync_fact_payment_invoice():

        try:
            target_hook = get_pg_hook("target_conn_id_dw")

            log.info(f"=== Sync Start: fact_payment_invoice Fact ===")
            
            # Get Last Sync ts
            last = get_last_sync_silver_fact_table(pg_hook=target_hook, target_schema="silver", target_table="fact_payment_invoice")
            last_synced_at = last["last_synced_at"]
            
            sql = f"""SELECT silver.fn_sync_fact_payment_invoice('{last_synced_at}')"""

            fn_exec_status = execute_function(pg_hook=target_hook, function_call_sql=sql)

            if fn_exec_status == 0:
                raise Exception(f"Error is orccured: {sql} ")
            
            # else:
            #     # Update last_sync_at in etl water mark table
            #     last_synced_date = datetime.utcnow() - timedelta(minutes=overlap_minutes)
            #     update_last_sync_date = update_last_sync_silver_fact_table(pg_hook=target_hook, target_schema="silver", target_table="fact_orders_stats", last_synced_at=last_synced_date)

            log.info(f"=== Sync End: fact_payment_invoice Fact ===")
        
        except Exception as e:
            log.error(f"ERROR: {e}", exc_info=True)
            raise

    # Sync fact_hub_performance Table
    def sync_fact_hub_performance():

        try:
            target_hook = get_pg_hook("target_conn_id_dw")

            log.info(f"=== Sync Start: fact_hub_performance Fact ===")
            
            # Get Last Sync ts
            last = get_last_sync_silver_fact_table(pg_hook=target_hook, target_schema="silver", target_table="fact_hub_performance")
            last_synced_at = last["last_synced_at"]
            
            sql = f"""SELECT silver.fn_sync_fact_hub_performance('{last_synced_at}')"""

            fn_exec_status = execute_function(pg_hook=target_hook, function_call_sql=sql)

            if fn_exec_status == 0:
                raise Exception(f"Error is orccured: {sql} ")
            
            # else:
            #     # Update last_sync_at in etl water mark table
            #     last_synced_date = datetime.utcnow() - timedelta(minutes=overlap_minutes)
            #     update_last_sync_date = update_last_sync_silver_fact_table(pg_hook=target_hook, target_schema="silver", target_table="fact_orders_stats", last_synced_at=last_synced_date)

            log.info(f"=== Sync End: fact_payment_invoice Fact ===")
        
        except Exception as e:
            log.error(f"ERROR: {e}", exc_info=True)
            raise

    # Refresh mv_fact_business_stats
    def refresh_mv_fact_business_stats(**context):
        try:
            target_hook = get_pg_hook("target_conn_id_dw")
            
            log.info(f"=== Sync Start: mv_bussiness_stats ===")

            sql = "REFRESH MATERIALIZED VIEW gold.mv_bussiness_stats;"

            fn_exec_status = refresh_materialized_view(pg_hook=target_hook, mv_refresh_sql=sql)

            if fn_exec_status is None:
                raise Exception(f"Error is orccured: {sql} ")

            log.info(f"=== Sync End: mv_bussiness_stats ===")

        
        except Exception as e:
            log.error(f"ERROR: {e}", exc_info=True)
            raise
    
            
    # Sync fact_business_stats fact Table
    def sync_fact_business_stats():

        try:
            target_hook = get_pg_hook("target_conn_id_dw")

            log.info(f"=== Sync Start: fact_business_stats Fact ===")
            
            # Get Last Sync ts
            last = get_last_sync_silver_fact_table(pg_hook=target_hook, target_schema="silver", target_table="fact_business_stats")
            last_synced_at = last["last_synced_at"]
            
            sql = f"""SELECT silver.fn_sync_fact_business_stats('{last_synced_at}')"""

            fn_exec_status = execute_function(pg_hook=target_hook, function_call_sql=sql)

            if fn_exec_status == 0:
                raise Exception(f"Error is orccured: {sql} ")
            
            # else:
            #     # Update last_sync_at in etl water mark table
            #     last_synced_date = datetime.utcnow() - timedelta(minutes=overlap_minutes)
            #     update_last_sync_date = update_last_sync_silver_fact_table(pg_hook=target_hook, target_schema="silver", target_table="fact_orders_stats", last_synced_at=last_synced_date)

            log.info(f"=== Sync End: fact_business_stats Fact ===")
        
        except Exception as e:
            log.error(f"ERROR: {e}", exc_info=True)
            raise

    # # refesh mv_order_delivery_stats
    # def refresh_mv_order_delivery_stats(**context):
    #     try:
    #         target_hook = get_pg_hook("target_conn_id_dw")
            
    #         log.info(f"=== Sync Start: mv_order_delivery_status ===")

    #         sql = "REFRESH MATERIALIZED VIEW gold.mv_order_delivery_status;"

    #         fn_exec_status = refresh_materialized_view(pg_hook=target_hook, mv_refresh_sql=sql)

    #         if fn_exec_status is None:
    #             raise Exception(f"Error is orccured: {sql} ")

    #         log.info(f"=== Sync End: mv_order_delivery_status ===")

        
    #     except Exception as e:
    #         log.error(f"ERROR: {e}", exc_info=True)
    #         raise
    
    # Sync Dimension Tables
    def sync_dimension_tables():

        try:
            target_hook = get_pg_hook("target_conn_id_dw")

            log.info(f"=== Sync Start: dimension tables ===")
            
            sql = f"""SELECT silver.fn_sync_dimension_tables()"""

            fn_exec_status = execute_function(pg_hook=target_hook, function_call_sql=sql)

            if fn_exec_status == 0:
                raise Exception(f"Error is orccured: {sql} ")
            
            # else:
            #     # Update last_sync_at in etl water mark table
            #     last_synced_date = datetime.utcnow() - timedelta(minutes=overlap_minutes)
            #     update_last_sync_date = update_last_sync_silver_fact_table(pg_hook=target_hook, target_schema="silver", target_table="fact_orders_stats", last_synced_at=last_synced_date)

            log.info(f"=== Sync End: dimension tables ===")
        
        except Exception as e:
            log.error(f"ERROR: {e}", exc_info=True)
            raise
    
    # Sync dim_order_invoice_transaction Tables
    def sync_dim_order_invoice_transaction():

        try:
            target_hook = get_pg_hook("target_conn_id_dw")

            log.info(f"=== Sync Start: dim_order_invoice_transaction tables ===")
            
            sql = f"""SELECT silver.fn_sync_dim_order_invoice_transaction()"""

            fn_exec_status = execute_function(pg_hook=target_hook, function_call_sql=sql)

            if fn_exec_status == 0:
                raise Exception(f"Error is orccured: {sql} ")
            
            # else:
            #     # Update last_sync_at in etl water mark table
            #     last_synced_date = datetime.utcnow() - timedelta(minutes=overlap_minutes)
            #     update_last_sync_date = update_last_sync_silver_fact_table(pg_hook=target_hook, target_schema="silver", target_table="fact_orders_stats", last_synced_at=last_synced_date)

            log.info(f"=== Sync End: dim_order_invoice_transaction tables ===")
        
        except Exception as e:
            log.error(f"ERROR: {e}", exc_info=True)
            raise
    
    # Sync dim_payment_invoice_transaction Tables
    def sync_dim_payment_invoice_transaction():

        try:
            target_hook = get_pg_hook("target_conn_id_dw")

            log.info(f"=== Sync Start: dim_payment_invoice_transaction tables ===")
            
            sql = f"""SELECT silver.fn_sync_dim_payment_invoice_transaction()"""

            fn_exec_status = execute_function(pg_hook=target_hook, function_call_sql=sql)

            if fn_exec_status == 0:
                raise Exception(f"Error is orccured: {sql} ")
            
            # else:
            #     # Update last_sync_at in etl water mark table
            #     last_synced_date = datetime.utcnow() - timedelta(minutes=overlap_minutes)
            #     update_last_sync_date = update_last_sync_silver_fact_table(pg_hook=target_hook, target_schema="silver", target_table="fact_orders_stats", last_synced_at=last_synced_date)

            log.info(f"=== Sync End: dim_payment_invoice_transaction tables ===")
        
        except Exception as e:
            log.error(f"ERROR: {e}", exc_info=True)
            raise

    # Sync fact_orders_status Fact Table
    def sync_fact_order_stats():

        try:
            target_hook = get_pg_hook("target_conn_id_dw")

            log.info(f"=== Sync Start: Order Stats Fact ===")
            
            # Get Last Sync ts
            last = get_last_sync_silver_fact_table(pg_hook=target_hook, target_schema="silver", target_table="fact_orders_status")
            last_synced_at = last["last_synced_at"]
            
            sql = f"""SELECT silver.fn_sync_fact_order_status('{last_synced_at}')"""

            fn_exec_status = execute_function(pg_hook=target_hook, function_call_sql=sql)

            if fn_exec_status == 0:
                raise Exception(f"Error is orccured: {sql} ")
            
            # else:
            #     # Update last_sync_at in etl water mark table
            #     last_synced_date = datetime.utcnow() - timedelta(minutes=overlap_minutes)
            #     update_last_sync_date = update_last_sync_silver_fact_table(pg_hook=target_hook, target_schema="silver", target_table="fact_orders_stats", last_synced_at=last_synced_date)

            log.info(f"=== Sync End: Order Stats Fact ===")
        
        except Exception as e:
            log.error(f"ERROR: {e}", exc_info=True)
            raise


    # Sync Raw Tables      
    def sync_single_table(mapping: dict, **context):
        source_hook = get_pg_hook(mapping["source_conn_id"])
        target_hook = get_pg_hook(mapping["target_conn_id"])

        src_label = f"{mapping['source_conn_id']}:{mapping['source_schema']}.{mapping['source_table']}"
        tgt_label = f"{mapping['target_conn_id']}:{mapping['target_schema']}.{mapping['target_table']}"

        log.info(f"=== Sync Start: {src_label} → {tgt_label} ===")

        try:

            last = get_last_sync_info(
                target_hook,
                mapping["target_db_name"],
                mapping["target_schema"],
                mapping["target_table"]
            )

            last_synced_at = last["last_synced_at"]

            # ensure schema exists
            schema_status = ensure_target_schema_exists(target_hook, mapping)

            if schema_status is None:
                raise Exception(f"Unable to create {mapping["target_table"]} table !!!")

            etl_stats = upsert_etl_stats(target_hook, mapping, status="RUNNING")

            if etl_stats is None:
                raise Exception("Unable to store ETL status in water mark table (etl.etl_stats) !!!")
            
            status="SUCCESS"

            src_max_ts = get_source_max_updated(source_hook, mapping)
            src_count = get_source_row_count_by_timestamp(source_hook, mapping, last_synced_at)

            # Get PK and column metadata
            cols = get_source_columns_with_details(source_hook, mapping)
            pk_cols = get_primary_key_columns(source_hook, mapping)

            # Auto-create target table if needed
            schema_sync_status = ensure_target_table_exists_and_add_missing_columns(
                target_hook, mapping, cols, pk_cols
            )

            total_upserted = 0
            affected = 0
           
            if schema_sync_status is None:
                raise Exception(f"Unable to Sync schema on {mapping["target_table"]} table !!!")

            for chunk in fetch_changed_rows(source_hook, mapping, since_ts=last_synced_at, chunk_size=CHUNK_SIZE):
                affected, upsert_status = upsert_rows_to_target(target_hook, mapping, chunk)
                if upsert_status:
                    total_upserted += (affected if isinstance(affected, int) else 0)
                else:
                    raise Exception(f"Exception occurred during data syncing on {mapping["target_table"]} table !!!")

            # Deduct overlap minutes to minilize the data lose
            now_ts = datetime.utcnow() - timedelta(minutes=overlap_minutes)

            # deduct 6 hours to convert into GMT
            now_ts = datetime.utcnow() - timedelta(hours=6)
       
            target_count, target_row_cnt_status = get_target_row_count(
                    get_pg_hook(mapping["target_conn_id"]),
                    mapping,
                    last_synced_at
                    )
            
            if target_row_cnt_status is None:
                target_count = 0
                raise Exception(f"Failed to count rows in {mapping["target_table"]} target table !!!")

            # Successful 
            upsert_etl_stats(
                target_hook, 
                mapping,
                last_synced_at=now_ts,
                last_source_max_updated=src_max_ts,
                last_source_row_count=src_count,
                last_target_row_count=target_count,
                status=status,
                last_error=None
            )

            log.info(f"=== Sync Finished: {src_label} → {tgt_label} | Upserted {total_upserted} rows ===")

        except Exception as e:
            tb = traceback.format_exc()
            log.error(f"Sync FAILED for {src_label} → {tgt_label}: {e}\n{tb}")

            upsert_etl_stats(
                target_hook,
                mapping,
                status="FAILED",
                last_error=str(e)[:1000]
            )
            raise

    log.info(f"XML path = {XML_PATH}")        
    mappings = parse_tables_map(XML_PATH)

    # Collect all sync tasks
    sync_tasks = []

    for m in mappings:
        task_id = (
            f"sync__{m['source_conn_id']}__{m['source_schema']}__{m['source_table']}"
            f"__to__{m['target_conn_id']}__{m['target_schema']}__{m['target_table']}"
        )

        log.info(f"Task ID: {task_id}")

        t = PythonOperator(
            task_id=task_id,
            python_callable=sync_single_table,
            op_kwargs={"mapping": m},
        )

        sync_tasks.append(t)

    run_sync_dimension_tables = PythonOperator(
        task_id="sync_dimension_tables",
        python_callable=sync_dimension_tables,
    )

    run_sync_dim_order_invoice_transaction = PythonOperator(
        task_id="sync_dim_order_invoice_transaction",
        python_callable=sync_dim_order_invoice_transaction,
    )

    run_sync_dim_payment_invoice_transaction = PythonOperator(
        task_id="sync_dim_payment_invoice_transaction",
        python_callable=sync_dim_payment_invoice_transaction,
    )

    run_fact_order_stats_sync = PythonOperator(
        task_id="run_fact_order_stats",
        python_callable=sync_fact_order_stats,
    )

    run_fact_business_stats_sync = PythonOperator(
        task_id="sync_fact_business_stats",
        python_callable=sync_fact_business_stats,
    )

    run_sync_fact_daily_orders_summary = PythonOperator(
        task_id="sync_fact_daily_orders_summary",
        python_callable=sync_fact_daily_orders_summary,
    )

    run_sync_fact_order_invoice = PythonOperator(
        task_id="sync_fact_order_invoice",
        python_callable=sync_fact_order_invoice,
    )

    run_sync_fact_payment_invoice = PythonOperator(
        task_id="sync_fact_payment_invoice",
        python_callable=sync_fact_payment_invoice,
    )

    run_sync_fact_hub_performance = PythonOperator(
        task_id="run_sync_fact_hub_performance",
        python_callable=sync_fact_hub_performance,
    )

    # refresh_mv_order_delivery_stats = PythonOperator(
    #     task_id="refresh_mv_order_delivery_stats",
    #     python_callable=refresh_mv_order_delivery_stats,
    # )

    refresh_mv_fact_business_stats = PythonOperator(
        task_id="refresh_mv_fact_business_stats",
        python_callable=refresh_mv_fact_business_stats,
    )

    refresh_mv_orders_daily_summary = PythonOperator(
        task_id="refresh_mv_orders_daily_summary",
        python_callable=refresh_mv_orders_daily_summary,
    )

    refresh_mv_agent_report = PythonOperator(
        task_id="refresh_mv_agent_report",
        python_callable=refresh_mv_agent_report,
    )

    refresh_mv_daily_order_invoice = PythonOperator(
        task_id="refresh_mv_daily_order_invoice",
        python_callable=refresh_mv_daily_order_invoice,
    )

    refresh_mv_daily_payment_invoice = PythonOperator(
        task_id="refresh_mv_daily_payment_invoice",
        python_callable=refresh_mv_daily_payment_invoice,
    )

    refresh_mv_order_global_report = PythonOperator(
        task_id="refresh_mv_order_global_report",
        python_callable=refresh_mv_order_global_report,
    )

    refresh_mv_delivery_status_last_7days = PythonOperator(
        task_id="refresh_mv_delivery_status_last_7days",
        python_callable=refresh_mv_delivery_status_last_7days,
    )

    refresh_mv_delivery_aging_last_7days = PythonOperator(
        task_id="refresh_mv_delivery_aging_last_7days",
        python_callable=refresh_mv_delivery_aging_last_7days,
    )

    # Set dependencies: ALL sync tasks → run_fact_sync
    for t in sync_tasks:
        t >> run_sync_dimension_tables
        # t >> run_fact_business_stats_sync
    
    # Sync fact tables
    run_sync_dimension_tables >> run_sync_dim_order_invoice_transaction
    run_sync_dim_order_invoice_transaction >> run_sync_dim_payment_invoice_transaction
    run_sync_dim_payment_invoice_transaction >> run_fact_order_stats_sync
    run_fact_order_stats_sync >> run_fact_business_stats_sync
    run_fact_business_stats_sync >> run_sync_fact_daily_orders_summary

    run_sync_fact_daily_orders_summary >> run_sync_fact_order_invoice
    run_sync_fact_order_invoice >> run_sync_fact_payment_invoice
    run_sync_fact_payment_invoice >> run_sync_fact_hub_performance

    # Run materialized view refresh after stats sync
    run_sync_fact_hub_performance >> refresh_mv_fact_business_stats
    refresh_mv_fact_business_stats >> refresh_mv_orders_daily_summary
    refresh_mv_orders_daily_summary >> refresh_mv_agent_report
    refresh_mv_agent_report >> refresh_mv_daily_order_invoice
    refresh_mv_daily_order_invoice >> refresh_mv_daily_payment_invoice
    refresh_mv_daily_payment_invoice >> refresh_mv_order_global_report
    refresh_mv_order_global_report >> refresh_mv_delivery_status_last_7days
    refresh_mv_delivery_status_last_7days >> refresh_mv_delivery_aging_last_7days