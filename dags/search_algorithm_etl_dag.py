from __future__ import annotations

import pendulum

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.dummy import DummyOperator
from airflow.providers.amazon.aws.transfers.s3_to_sql import S3ToSqlOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
import csv
from io import StringIO
from datetime import datetime, timedelta
from airflow.utils.dates import days_ago
from airflow.operators.email_operator import EmailOperator
#from kafka import KafkaProducer
import json
from airflow.models import Variable
from decimal import Decimal
from datetime import datetime, date


# Schedule for DAG execution, run every night at 1PM
SCHEDULE_INTERVAL = '10 */3 * * *' # Run every 3 hours

# Email notification
DATA_ENGINEER_EMAIL_GROUP = "obaidul.haque@technonext.com"

# Define your S3 details
S3_CONN_ID = "s3_product_view_count"  # The ID you set in step 1
S3_BUCKET = "product-migration"
S3_FOLDER_KEY = "product_view_count/product_view_count.csv" # The specific folder path (key prefix)

POSTGRES_TABLE = "stg.product_views"
COLUMN_LIST=['product_id', 'view_count', 'updated_at']

# Read Kafka variables from Airflow
KAFKA_BOOTSTRAP_SERVERS = Variable.get(
    "KAFKA_BOOTSTRAP_SERVERS",
    deserialize_json=True  # because it's a JSON list
)
KAFKA_TOPIC = Variable.get("KAFKA_TOPIC")

KAFKA_MSG_BATCH_SIZE = int(Variable.get("KAFKA_MSG_BATCH_SIZE"))

TARGET_DATABASE_CONN="pg_advanced_search"

def product_view_csv_parser(file_path):
    """
    Reads the CSV file from the given path, parses all lines, skips the header, 
    and yields a list of data rows (product_id, view_count, updated_at) to the operator.
    """
    # Initialize a list to hold all parsed rows
    rows = []
    
    # Airflow automatically injects the timestamp via Jinja templating, 
    # so we need a placeholder to fill the third column (updated_at).
    # We will use the execution_date macro placeholder here.
    # Note: If this still fails, you may need to use a fixed string like 'PLACEHOLDER' 
    # and rely entirely on sql_copy_options. For now, we return 2 columns in the rows 
    # and rely on the sql_copy_options parameter to supply the 3rd column.

    updated_at = datetime.now()

    with open(file_path, 'r') as f:
        reader = csv.reader(f)
        
        # Skip the header row
        next(reader, None)

        for row in reader:
            if len(row) == 2:
                try:
                    # Clean up quotes if any (csv reader handles it but a safety check)
                    product_id = int(row[0].strip())
                    view_count = int(row[1].strip())
                    
                    # Return only the data fields. The timestamp will be injected 
                    # by the operator's `sql_copy_options`.
                    rows.append((product_id, view_count, updated_at))
                except ValueError:
                    # Skip corrupted data rows
                    continue
                    
    # Return the entire list of rows to the operator
    return rows

# --- Python Functions for Extraction and Transformation ---

def extract_and_load_postgres(source_conn_id: str, source_query: str, target_conn_id: str, target_table: str):
    """
    Extracts data from a source Postgres table and loads it into a target table
    in the destination Postgres database using a PostgresHook.
    """
    # 1. Get data from the source database
    source_hook = PostgresHook(postgres_conn_id=source_conn_id)
    sql_query = source_query
    
    # Fetch all records
    records = source_hook.get_records(sql=sql_query)
    
    # 2. Get hook for the target database
    target_hook = PostgresHook(postgres_conn_id=target_conn_id)
    
    # 4. Insert data into the target table
    # This method is good for small to medium datasets
    target_hook.insert_rows(
        table=target_table,
        rows=records,
        commit_every=1000  # Commit in batches of 1000 rows
    )
    print(f"Successfully loaded {len(records)} records from {source_conn_id}:{source_query} into {target_table}.")

    # The actual execution happens when Airflow calls the task function, 
    # but we define it here for clarity. In the DAG, we'll use a separate instance.

# def json_serializer(obj):
#     if isinstance(obj, Decimal):
#         return float(obj)   # or str(obj) if precision is critical
#     raise TypeError(f"Type {type(obj)} not serializable")

def json_serializer(obj):
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if isinstance(obj, Decimal):
        return float(obj)
    raise TypeError(f"Type {type(obj)} not serializable")


# Get Last Sync Value
def get_last_sync_date(target_table: str) -> str:

    try:
        sql = f"""
        SELECT last_sync_at
        FROM public.etl
        WHERE LOWER(table_name) = LOWER('{target_table}')
        """

        pg_hook = PostgresHook(postgres_conn_id=TARGET_DATABASE_CONN)
        conn = pg_hook.get_conn()
        cur = conn.cursor()
        cur.execute(sql)
        row = cur.fetchone()
        cur.close()
        conn.close()

        # time_string = "1900-01-01 01:01:01.000000+00:00"
        # deafult_ts = datetime.fromisoformat(time_string)

        if row:
            return row[0]

    except Exception as e:
        print(f"ERROR: {e}")
        return None
    
def update_last_sync_date(target_table, last_synced_at)->bool:

    try:

        sql =f"""
            INSERT INTO public.etl(table_name, last_sync_at) 
            SELECT '{target_table}' AS table_name,
            '{last_synced_at}'::timestamp with time zone AS last_sync_at
            ON CONFLICT(table_name) DO UPDATE 
            SET last_sync_at='{last_synced_at}'::timestamp with time zone;
"""
        print(f"sql: {sql}")

        pg_hook = PostgresHook(postgres_conn_id=TARGET_DATABASE_CONN)
        conn = pg_hook.get_conn()
        cur = conn.cursor()
        cur.execute(sql)
        conn.commit()
        cur.close()
        conn.close()

        return True

    except Exception as e:
        print(f"ERROR: {e}")
        return False
    
def publish_product_scores_to_kafka():
    from kafka import KafkaProducer  # runtime import
    import json

    # Kafka producer Instance
    producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v, default=json_serializer).encode("utf-8"),
            key_serializer=lambda v: str(v).encode("utf-8") if v else None,

            # Batching configs
            # batch_size=KAFKA_MSG_BATCH_SIZE * 1024,   # 1000 KB (NOT number of messages)
            linger_ms=100,           # wait up to 100 ms to form batch
            compression_type="gzip", # optional but recommended

            acks="all",
            retries=3,
        )
    
    # Connect to PostgreSQL
    pg_hook = PostgresHook(postgres_conn_id=TARGET_DATABASE_CONN)
    conn = pg_hook.get_conn()

    last_sync_at=get_last_sync_date(target_table='public.product_score')
    current_ts = datetime.now()

    if last_sync_at is None:
        last_sync_at=datetime.fromisoformat("1900-01-01 01:01:01.000000+00:00")

    print(f"Last Sync Timestamp: {last_sync_at}")

    # product score cursor
    cursor = conn.cursor(name="product_score_cursor")
    cursor.itersize = KAFKA_MSG_BATCH_SIZE

    query = f"""
        SELECT product_id AS "id", score AS "baseScore"
        FROM public.product_score
        WHERE updated_at>='{last_sync_at}'::timestamp with time zone
        ORDER BY product_id ASC
        ;
    """

    print(f"Query: {query}")

    cursor.execute(query)
    # colnames = [desc[0] for desc in cursor.description]
    colnames = ["id", "baseScore"]

    total_sent = 0
    batch_no = 0

    print(f"Sending messages to Kafka topic '{KAFKA_TOPIC}'")
    while True:
        rows = cursor.fetchmany(KAFKA_MSG_BATCH_SIZE)
        if not rows:
            break

        batch_payload = [
            dict(zip(colnames, row))
            for row in rows
        ]

        # ONE Kafka message = 100 JSON records
        producer.send(
            topic=KAFKA_TOPIC, 
            value=batch_payload
            )
        
        producer.flush()

        batch_no += 1
        total_sent += len(batch_payload)

        if batch_no % (KAFKA_MSG_BATCH_SIZE*10) == 0:
            print(f"Sent ({total_sent} messages ...)")


    producer.flush()
    producer.close()
    cursor.close()
   

    print(f"Published {total_sent} messages to Kafka topic '{KAFKA_TOPIC}'")

    # Update Sync Date
    is_update_sync_date = update_last_sync_date(last_synced_at=current_ts, target_table='public.product_score')
    
# --- DAG Definition ---

# Default DAG arguments
default_args = {
    'owner': 'root',
    'depends_on_past': False,
    'email': [DATA_ENGINEER_EMAIL_GROUP],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'start_date': days_ago(1),  # Update with your preferred start date
    'on_failure_callback': lambda context: send_email(DATA_ENGINEER_EMAIL_GROUP,
                                                      "Airflow DAG Failure Notification",
                                                      f"DAG {context['task_instance'].dag_id} failed"),
}

with DAG(
    dag_id="search_algorithm_data_pipeline",
    default_args=default_args,
    schedule_interval=SCHEDULE_INTERVAL,
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["etl", "postgres", "s3"],
) as dag:
    
    start = DummyOperator(task_id="start_etl")
    TARGET_DATABASE_CONN="pg_advanced_search"
    # 1. Setup/Schema Tasks (Only run once or on first run)
    
    # Task to ensure staging tables exist in the target database
    create_stagging_tables = PostgresOperator(
        task_id="create_stagging_tables",
        pool="db_heavy_pool",
        postgres_conn_id=TARGET_DATABASE_CONN,
        sql="""
            CREATE TABLE IF NOT EXISTS stg.product_info
            (
                product_id integer NOT NULL,
                seller_id	integer NOT NULL,
                category_id	integer NOT NULL,
                stock_count	integer NOT NULL,
                is_new_product boolean NOT NULL,
                updated_at timestamp with time zone
            );

        CREATE TABLE IF NOT EXISTS stg.seller_review
            (
                seller_id	integer NOT NULL,
                seller_rating	numeric(10,2) NOT NULL,
                updated_at timestamp with time zone
            );

        CREATE TABLE IF NOT EXISTS stg.customer_review
        (
            product_id	integer NOT NULL,
            product_rating	numeric(10,2) NOT NULL,
            updated_at timestamp with time zone
        );

        CREATE TABLE IF NOT EXISTS stg.order_items
        (
            product_id	integer NOT NULL,
            gmv	numeric(10,2) NOT NULL,
            order_item_count integer NOT NULL,
            updated_at timestamp with time zone
        );

        CREATE TABLE IF NOT EXISTS stg.deal_bowl
        (
            product_id	integer NOT NULL,
            is_in_deal_bowl boolean NOT NULL,
            updated_at timestamp with time zone
        );

        CREATE TABLE IF NOT EXISTS stg.product_views
        (
            product_id	integer NOT NULL,
            view_count integer NOT NULL,
            updated_at timestamp with time zone
        );

        """,
    )

    # Clean Stage data
    clean_stage_tables = PostgresOperator(
        task_id="clean_stage_tables",
        postgres_conn_id=TARGET_DATABASE_CONN,
        sql="""
        TRUNCATE TABLE stg.product_info;
        TRUNCATE TABLE stg.seller_review;
        TRUNCATE TABLE stg.customer_review;
        TRUNCATE TABLE stg.order_items;
        TRUNCATE TABLE stg.deal_bowl;
        TRUNCATE TABLE stg.product_views;
        """,
    )

    create_target_tables = PostgresOperator(
        task_id="create_target_tables",
        pool="db_heavy_pool",
        postgres_conn_id=TARGET_DATABASE_CONN,
        sql="""
            CREATE TABLE IF NOT EXISTS public.product_stats
            (
                    product_id integer NOT NULL PRIMARY KEY,
                    seller_id integer NOT NULL,
                    category_id integer NOT NULL,
                    product_rating numeric(10,2) NOT NULL,
                    seller_rating numeric(10,2) NOT NULL,
                    product_view_count integer NOT NULL,
                    gmv numeric(10,2) NOT NULL,
                    order_item_count integer NOT NULL,
                    stock_count integer NOT NULL,
                    is_new_product boolean NOT NULL,
                    is_in_deal_bowl boolean NOT NULL,
                    updated_at timestamp with time zone
            );
            

            CREATE TABLE IF NOT EXISTS public.category_states
            (
                category_id integer NOT NULL PRIMARY KEY,
                avg_rating numeric(10,2) NOT NULL,
                avg_view_count numeric(10,2) NOT NULL,
                avg_gmv numeric(10,2) NOT NULL,
                avg_order_item_count numeric(10,2) NOT NULL,
                updated_at timestamp with time zone
            );

            CREATE TABLE IF NOT EXISTS public.product_score
            (
                product_id integer NOT NULL PRIMARY KEY,
                score numeric(10,2) NOT NULL,
                updated_at timestamp with time zone
            );

            CREATE TABLE IF NOT EXISTS public.etl(
                table_name VARCHAR(100) NOT NULL PRIMARY KEY,
                last_sync_at timestamp with time zone NOT NULL
            );


        """,
    )

    # Load S3 Data
    # Use the S3ToSqlOperator for efficiency
    s3_to_pg_product_views = S3ToSqlOperator(
        task_id="load_product_view_count",
        pool="db_heavy_pool",
        # Connection IDs
        aws_conn_id=S3_CONN_ID,
        sql_conn_id=TARGET_DATABASE_CONN,

        # S3 configuration
        s3_bucket=S3_BUCKET,
        s3_key=S3_FOLDER_KEY,

        # PostgreSQL configuration
        table=POSTGRES_TABLE,
        column_list=COLUMN_LIST,
        
        parser=product_view_csv_parser,
    )

    # 2. Extraction & Loading Tasks (E & L)

    # Load product_info stage
    product_info_query="""
SELECT "Id" AS product_id, 
"SellerId" AS seller_id,
"CategoryId" AS category_id,
"TotalStockQuantity" AS stock_count,
(CASE WHEN "CreatedAt">= NOW() - INTERVAL '7 days' THEN true else false END) AS is_new_product,
NOW() AS updated_at
FROM product."Product"
WHERE "ApprovalStatusId"=1;
"""
    load_product_info = PythonOperator(
        task_id="load_product_info",
        pool="db_heavy_pool",
        python_callable=extract_and_load_postgres,
        op_kwargs={
            "source_conn_id": "pg_product_customer",
            "source_query": product_info_query,
            "target_conn_id": TARGET_DATABASE_CONN,
            "target_table": "stg.product_info"
        },
    )

    # Load seller_review stage
    seller_review_query="""
SELECT "SellerId" AS seller_id,
AVG("Rating") AS seller_rating,
NOW() AS updated_at
FROM seller."SellerReview"
GROUP BY "SellerId";
"""
    load_seller_review = PythonOperator(
        task_id="load_seller_review",
        pool="db_heavy_pool",
        python_callable=extract_and_load_postgres,
        op_kwargs={
            "source_conn_id": "pg_product_customer",
            "source_query": seller_review_query,
            "target_conn_id": TARGET_DATABASE_CONN,
            "target_table": "stg.seller_review"
        },
    )

    # Load customer_review stage
    customer_review_query="""
SELECT "ProductId" AS product_id,
SUM("Rating")/COUNT(1) AS product_rating,
NOW() AS updated_at
FROM customer."CustomerReviews"
WHERE "IsAccepted" IS TRUE
GROUP BY "ProductId";
"""
    load_customer_review = PythonOperator(
        task_id="load_customer_review",
        pool="db_heavy_pool",
        python_callable=extract_and_load_postgres,
        op_kwargs={
            "source_conn_id": "pg_product_customer",
            "source_query": customer_review_query,
            "target_conn_id": TARGET_DATABASE_CONN,
            "target_table": "stg.customer_review"
        },
    )

    # Load order_items stage
    order_items_query="""
SELECT oi."ProductId" AS product_id,
SUM(oi."Price"*oi."Quantity") AS gmv,
SUM(oi."Quantity") AS order_item_count,
NOW() AS updated_at
FROM "OrderItems" AS oi
WHERE oi."CreatedAt">= NOW() - INTERVAL '7 days'
AND EXISTS (SELECT 1 FROM public."Orders" AS o WHERE o."Id"=oi."OrderId" AND o."PackageStatusId"!=23)
GROUP BY oi."ProductId"
"""
    load_order_items = PythonOperator(
        task_id="load_order_items",
        pool="db_heavy_pool",
        python_callable=extract_and_load_postgres,
        op_kwargs={
            "source_conn_id": "pg_logistics",
            "source_query": order_items_query,
            "target_conn_id": TARGET_DATABASE_CONN,
            "target_table": "stg.order_items"
        },
    )

    # Load deal_bowl stage
    deal_bowl_query="""
SELECT csp."ProductId" AS product_id,
true AS is_in_deal_bowl,
NOW() AS updated_at
FROM "CampaignSellerProducts" AS csp
WHERE EXISTS (SELECT 1 FROM public."Campaigns" AS c 
	WHERE c."Id"=csp."CampaignId" AND DATE_TRUNC('day', NOW()) BETWEEN c."StartDate" AND c."EndDate")
GROUP BY "ProductId";
"""
    load_deal_bowl = PythonOperator(
        task_id="load_deal_bowl",
        pool="db_heavy_pool",
        python_callable=extract_and_load_postgres,
        op_kwargs={
            "source_conn_id": "pg_product_customer",
            "source_query": deal_bowl_query,
            "target_conn_id": TARGET_DATABASE_CONN,
            "target_table": "stg.deal_bowl"
        },
    )

    # Extraction from S3 Parquet File
    # Note: S3ToSqlOperator works best with CSV/TSV. For Parquet, you might need 
    # a PythonOperator with pandas/pyarrow, but we use this simplified version.
    # If the file is Parquet, the 'file_format' should handle it (or use a dedicated 
    # Python task to read Parquet and load using PostgresHook).
    # extract_load_s3_parquet = S3ToSqlOperator(
    #     task_id="extract_load_s3_parquet",
    #     aws_conn_id='aws_default',
    #     s3_bucket='your-data-bucket-name',
    #     s3_key='products/recent_products.parquet', 
    #     sql_conn_id='postgres_target',
    #     table='staging_s3',
    #     file_format='parquet',  # This may require specific database support or custom implementation
    #     # The 'schema' and 'column_list' must match the S3 file and the staging_s3 table
    #     # We assume a clean match here for simplicity
    #     column_list=[
    #         "id", "product_name", "manufacturer", "load_date"
    #     ],
    #     replace=False, # Append data
    # )


    # # 3. Transformation Task (T)

    # Transform data from all staging tables into the final table
    transform_and_merge_product_stats = PostgresOperator(
        task_id="transform_and_merge_product_stats",
        postgres_conn_id=TARGET_DATABASE_CONN,
        pool="db_heavy_pool",
        sql="""
            TRUNCATE TABLE public.product_stats;
        
            -- Combine and clean data from the staging tables and insert into a final table
            INSERT INTO public.product_stats(
            product_id, seller_id, category_id, product_rating, seller_rating, product_view_count, 
            gmv, order_item_count, stock_count, is_new_product, is_in_deal_bowl, updated_at)
            SELECT p.product_id, p.seller_id, p.category_id,
            COALESCE(pr.product_rating,0) AS product_rating, 
            COALESCE(sr.seller_rating, 0) AS seller_rating, 
            COALESCE(v.view_count,0) as product_view_count,
            COALESCE(ord.gmv,0) AS gmv, 
            COALESCE(ord.order_item_count,0) AS order_item_count, 
            COALESCE(p.stock_count,0) AS stock_count, 
            COALESCE(p.is_new_product, false) AS is_new_product, 
            COALESCE(deal.is_in_deal_bowl,false) AS is_in_deal_bowl, now() AS updated_at
            FROM stg.product_info AS p
            LEFT JOIN stg.customer_review AS pr ON (pr.product_id=p.product_id)
            LEFT JOIN stg.seller_review AS sr ON (sr.seller_id=p.seller_id)
            LEFT JOIN stg.order_items AS ord ON (ord.product_id=p.product_id )
            LEFT JOIN stg.deal_bowl AS deal ON (deal.product_id=p.product_id)
            LEFT JOIN stg.product_views AS v ON (v.product_id=p.product_id)
            ;
        """,
        autocommit=True,
    )

    transform_and_merge_category_stats = PostgresOperator(
        task_id="transform_and_merge_category_stats",
        postgres_conn_id=TARGET_DATABASE_CONN,
        pool="db_heavy_pool",
        sql="""
            TRUNCATE TABLE public.category_states;

            INSERT INTO public.category_states(
            category_id, avg_rating, avg_view_count, 
            avg_gmv, avg_order_item_count, updated_at)
        WITH cat_avg_rating AS (
            SELECT category_id, 
            AVG(product_rating) AS avg_rating
            FROM public.product_stats
            WHERE product_rating>0
            GROUP BY category_id
        ),
		cat_avg_gmv AS (
			SELECT category_id, 
            AVG(gmv) AS avg_gmv
            FROM public.product_stats
            WHERE gmv>0
            GROUP BY category_id
		),
		cat_avg_order_count AS (
			SELECT category_id, 
            AVG(order_item_count) AS avg_order_count
            FROM public.product_stats
            WHERE order_item_count>0
            GROUP BY category_id
		),
		cat_avg_view_count AS (
			SELECT category_id, 
            AVG(product_view_count) AS avg_product_view_count
            FROM public.product_stats
            WHERE product_view_count>0
            GROUP BY category_id
		)
		
        SELECT category_id, 
        ROUND(COALESCE((SELECT avg_rating FROM cat_avg_rating AS c WHERE c.category_id=p.category_id),0),2) AS product_rating,
		ROUND(COALESCE((SELECT avg_product_view_count FROM cat_avg_view_count AS c WHERE c.category_id=p.category_id),0),2) AS avg_view_count,
		ROUND(COALESCE((SELECT avg_gmv FROM cat_avg_gmv AS c WHERE c.category_id=p.category_id),0),2) AS avg_gmv,
		ROUND(COALESCE((SELECT avg_order_count FROM cat_avg_order_count AS c WHERE c.category_id=p.category_id),0),2) AS avg_order_item_count,
        now() AS updated_at
        FROM public.product_stats AS p
        GROUP BY p.category_id
            ;
        """,
        autocommit=True,
    )

    calculate_product_score = PostgresOperator(
        task_id="calculate_product_score",
        pool="db_heavy_pool",
        postgres_conn_id=TARGET_DATABASE_CONN,
        sql="""

            -- Product Score
            WITH seller_rating AS (
                SELECT seller_id,category_id, AVG(seller_rating) AS seller_rating
                FROM public.product_stats
                GROUP BY seller_id,category_id
                ),
                weight_param AS (
                SELECT
                MAX(weight_value) FILTER (WHERE rank_parameter='Seller Rating')  AS wv_seller_rating,
                MAX(weight_value) FILTER (WHERE rank_parameter='Stock Availability') AS wv_stock_availability,
                MAX(weight_value) FILTER (WHERE rank_parameter='GMV') AS wv_gmv,
                MAX(weight_value) FILTER (WHERE rank_parameter='Newly Added Product') AS wv_newly_added_product,
                MAX(weight_value) FILTER (WHERE rank_parameter='Order Count') AS wv_order_count,
                MAX(weight_value) FILTER (WHERE rank_parameter='Product Rating') AS wv_product_rating,
                MAX(weight_value) FILTER (WHERE rank_parameter='Deal Bowl Participation') AS wv_deal_bowl_participation,
                MAX(weight_value) FILTER (WHERE rank_parameter='CTR/Product View') AS wv_product_view
                FROM advanced_search.algorithm_parameters
                ),
                product_score_cte AS (
                SELECT product_id, category_id, (score_rating + score_seller_rating + score_product_view + score_gmv 
                + score_order_item_count + score_stock_count + score_new_product + score_deal_bowl) AS score
				FROM (
				    SELECT product_id, category_id, 
						(CASE WHEN score_rating>wv_product_rating THEN wv_product_rating ELSE score_rating END) AS score_rating,
						(CASE WHEN score_seller_rating>wv_seller_rating THEN wv_seller_rating ELSE score_seller_rating END) AS score_seller_rating,
						(CASE WHEN score_product_view>wv_product_view THEN wv_product_view ELSE score_product_view END) AS score_product_view,
						(CASE WHEN score_gmv>wv_gmv THEN wv_gmv ELSE score_gmv END) AS score_gmv,
						(CASE WHEN score_order_item_count>wv_order_count THEN wv_order_count ELSE score_order_item_count END) AS score_order_item_count,
						(CASE WHEN score_stock_count>wv_stock_availability THEN wv_stock_availability ELSE score_stock_count END) AS score_stock_count,
						(CASE WHEN score_new_product>wv_newly_added_product THEN wv_newly_added_product ELSE score_new_product END) AS score_new_product,
						(CASE WHEN score_deal_bowl>wv_deal_bowl_participation THEN wv_deal_bowl_participation ELSE score_deal_bowl END) AS score_deal_bowl	
	                FROM 
					(
						SELECT p.product_id, 
	                        p.category_id, 
	                        (CASE WHEN c.avg_rating>0 THEN ROUND(COALESCE(p.product_rating,0)/COALESCE(c.avg_rating,0),2) ELSE 0 END)*COALESCE(wp.wv_product_rating,0)  AS score_rating, 
							COALESCE(wp.wv_product_rating,0) AS wv_product_rating,
	                        (CASE WHEN c.avg_rating>0 THEN ROUND(COALESCE(sr.seller_rating,0), 2) ELSE 0 END)*COALESCE(wp.wv_seller_rating,0) AS score_seller_rating, 
							COALESCE(wp.wv_seller_rating,0) AS wv_seller_rating,
	                        (CASE WHEN c.avg_view_count>0 THEN ROUND(COALESCE(p.product_view_count,0)/COALESCE(c.avg_view_count,0),2) ELSE 0 END)*COALESCE(wp.wv_product_view) AS score_product_view, 
							COALESCE(wp.wv_product_view) AS wv_product_view,
	                        (CASE WHEN c.avg_gmv>0 THEN ROUND(COALESCE(p.gmv,0)/COALESCE(c.avg_gmv,0), 2) ELSE 0 END)*COALESCE(wp.wv_gmv,0) AS score_gmv, 
							COALESCE(wp.wv_gmv,0) AS wv_gmv,
	                        (CASE WHEN c.avg_order_item_count>0 THEN ROUND(COALESCE(p.order_item_count,0)/COALESCE(c.avg_order_item_count,0),2) ELSE 0 END)*COALESCE(wp.wv_order_count, 0) AS score_order_item_count, 
							COALESCE(wp.wv_order_count, 0) AS wv_order_count, 
	                        (CASE 
                                  WHEN p.stock_count>100 THEN 1.0 
                                  WHEN p.stock_count>=51 AND p.stock_count<=100 THEN 0.55 
	                              WHEN p.stock_count>=11 AND p.stock_count<=50 THEN 0.40
	                              WHEN p.stock_count>=1 AND  p.stock_count<=10 THEN 0.25
	                            ELSE 0
	                        END)*COALESCE(wp.wv_stock_availability, 0) AS score_stock_count, 
							COALESCE(wp.wv_stock_availability, 0) AS wv_stock_availability, 
	                        (CASE WHEN p.is_new_product IS TRUE THEN COALESCE(wp.wv_newly_added_product,0) ELSE 0 END) AS score_new_product, 
							COALESCE(wp.wv_newly_added_product,0) AS wv_newly_added_product,
	                        (CASE WHEN p.is_in_deal_bowl IS TRUE THEN COALESCE(wp.wv_deal_bowl_participation,0) ELSE 0 END) AS score_deal_bowl, 
							COALESCE(wp.wv_deal_bowl_participation,0) AS wv_deal_bowl_participation
	                    FROM public.product_stats AS p
	                    INNER JOIN seller_rating AS sr ON (sr.seller_id=p.seller_id AND sr.category_id=p.category_id)
	                    INNER JOIN public.category_states AS c ON (c.category_id=p.category_id)
	                    CROSS JOIN weight_param AS wp
	                    ) tmp1
				    ) tmp2
                )
                -- Insert Final table
                INSERT INTO public.product_score AS t (product_id, score, updated_at)
                SELECT product_id, score, now()
                FROM product_score_cte
                ON CONFLICT (product_id) DO UPDATE
                SET
                    score = EXCLUDED.score,
                    updated_at = now()
                WHERE t.score IS DISTINCT FROM EXCLUDED.score;

                -- Update the boosted_products by Score
                UPDATE advanced_search.boosted_products AS upd
                SET base_score = sc.score,
                modified_at=current_timestamp
                FROM public.product_score AS sc
                WHERE sc.product_id=upd.product_id;

        """,
        autocommit=True,
    )

    publish_to_kafka = PythonOperator(
        task_id="publish_product_score_to_kafka",
        pool="db_heavy_pool",
        python_callable=publish_product_scores_to_kafka,
    )


    # merge_product_stats = PythonOperator(
    #     task_id="merge_product_stats",
    #     python_callable=transform_data_product_stats,
    #     op_kwargs={
    #         "target_conn_id": TARGET_DATABASE_CONN
    #     },
    # )

    # merge_category_stats = PythonOperator(
    #     task_id="merge_category_stats",
    #     python_callable=transform_data_category_stats,
    #     op_kwargs={
    #         "target_conn_id": TARGET_DATABASE_CONN
    #     },
    # )

    end = DummyOperator(task_id="end_etl")

    # --- Task Dependencies ---
    
    # 1. Start -> Setup
    start >> create_stagging_tables
    
    # 2. Setup -> All Extractions (Extraction tasks can run in parallel)
    [create_stagging_tables, create_target_tables] >> clean_stage_tables >> s3_to_pg_product_views >> [load_product_info, load_seller_review, load_customer_review, load_order_items, load_deal_bowl]
    
    # 3. All Extractions -> Transformation (Transformation waits for all data to be staged)
    [load_product_info, load_seller_review, load_customer_review, load_order_items, load_deal_bowl] >> transform_and_merge_product_stats >> transform_and_merge_category_stats
    
    # 4. Transformation -> End
    transform_and_merge_category_stats >> calculate_product_score >> publish_to_kafka >> end