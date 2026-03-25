# dags/sync_utils.py

import xml.etree.ElementTree as ET
from typing import List, Dict, Tuple, Optional
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
import psycopg2.extras
import datetime
import math
import logging
import traceback
from datetime import datetime
import json

log = logging.getLogger(__name__)

DEFAULT_CHUNK_SIZE = int(Variable.get("sync_chunk_size", default_var=1000))


# XML Validator
def validate_xml_config(xml_path: str):
    """
    Validates that the XML file is well-formed and contains all required attributes.
    Raises a ValueError if XML is invalid or incomplete.
    """

    try:
        # --- Step 1: Read raw file & strip BOM ---
        with open(xml_path, "rb") as f:
            raw = f.read()

        # Remove UTF-8 BOM if present
        if raw.startswith(b"\xef\xbb\xbf"):
            raw = raw[3:]

        try:
            root = ET.fromstring(raw)
        except ET.ParseError as e:
            raise ValueError(f"Invalid XML file '{xml_path}': {str(e)}") from e

        # --- Structure Validation ---
        if root.tag != "sources":
            raise ValueError("Root tag must be <sources>")

        required_source_attrs = ["source_db_name", "source_conn_id", "target_db_name", "target_conn_id"]
        required_table_attrs = ["source_schema", "source_table", "target_schema", "target_table", "pk_column", "ts_column"]

        for src in root.findall("source"):

            # Validate source attrs
            for attr in required_source_attrs:
                if attr not in src.attrib:
                    raise ValueError(f"<source> missing required attribute '{attr}'")

            # Validate table attributes
            for tbl in src.findall("table"):
                for attr in required_table_attrs:
                    if attr not in tbl.attrib:
                        raise ValueError(
                            f"<table> under source '{src.get('source_db_name')}' "
                            f"is missing required attribute '{attr}'"
                        )

        return True

    except Exception as e:
        log.error(f"ERROR: {e}", exc_info=True)
        return None
    
# -------------------------------------------------------------------------
# XML PARSER - Supports Multiple Source and Target Databases
# -------------------------------------------------------------------------
def parse_tables_map(xml_path: str) -> List[Dict]:
    """
    Parse XML mapping file with format:

    <sources>
        <source name="..." conn_id="...">
            <table
                source_schema="..."
                source_table="..."
                target_conn_id="..."
                target_schema="..."
                target_table="..."
                pk="..."
                last_updated="..."
            />
        </source>
    </sources>
    """
    try:
        valid_xml = validate_xml_config(xml_path)  # ← NEW STEP
        
        if valid_xml is None:
            raise Exception("Invalid XML configuration !!!")
        
        mappings = []
        
        tree = ET.parse(xml_path)
        root = tree.getroot()

        # tree = ET.parse(open(xml_path, "rb"))
        # root = tree.getroot()

        for src in root.findall("source"):
            source_conn_id = src.get("source_conn_id")
            source_db_name = src.get("source_db_name")

            target_conn_id = src.get("target_conn_id")
            target_db_name = src.get("source_db_name")

            for t in src.findall("table"):
                mappings.append({
                    "source_conn_id": source_conn_id,
                    "source_db_name": source_db_name,
                    "table_no": t.get("table_no"),
                    "source_schema": t.get("source_schema", "public"),
                    "source_table": t.get("source_table"),
                    "target_conn_id": target_conn_id,
                    "target_db_name": target_db_name,
                    "target_schema": t.get("target_schema", "staging"),
                    "target_table": t.get("target_table"),
                    "pk_column": t.get("pk_column"),
                    "ts_column": t.get("ts_column")
                })

        return mappings

    except Exception as e:
        log.error(f"ERROR: {e}", exc_info=True)
        return None
    
# -------------------------------------------------------------------------
# Postgres Connection Helper
# -------------------------------------------------------------------------
def get_pg_hook(conn_id: str) -> PostgresHook:
    return PostgresHook(postgres_conn_id=conn_id)


# -------------------------------------------------------------------------
# ETL Stats Functions
# -------------------------------------------------------------------------
def get_last_sync_info(pg_hook: PostgresHook, target_db_name:str, target_schema: str, target_table: str) -> Dict:

    try:
        sql = f"""
        SELECT last_synced_at, last_source_max_updated, last_source_row_count, status
        FROM etl.etl_stats
        WHERE LOWER(target_db_name) = LOWER('{target_db_name}') 
        AND LOWER(target_schema) = LOWER('{target_schema}') 
        AND LOWER(target_table) = LOWER('{target_table}')
        """

        log.info(f"Last Sync SQL: {sql}")

        conn = pg_hook.get_conn()
        cur = conn.cursor()
        cur.execute(sql)
        row = cur.fetchone()
        cur.close()

        time_string = "1900-01-01 01:01:01.000000+00:00"
        deafult_ts = datetime.fromisoformat(time_string)

        if not row:
            return {
                "last_synced_at": deafult_ts,
                "last_source_max_updated": deafult_ts,
                "last_source_row_count": 0,
                "status": None
            }

        return {
            "last_synced_at": row[0],
            "last_source_max_updated": row[1],
            "last_source_row_count": row[2],
            "status": row[3]
        }

    except Exception as e:
        log.error(f"ERROR: {e}", exc_info=True)
        return {
                "last_synced_at": deafult_ts,
                "last_source_max_updated": deafult_ts,
                "last_source_row_count": 0,
                "status": None
            }

# Get Last Sync Value
def get_last_sync_silver_fact_table(pg_hook: PostgresHook, target_schema: str, target_table: str) -> Dict:

    try:
        sql = f"""
        SELECT last_synced_at
        FROM etl.etl_stats_silver_facts
        WHERE LOWER(target_schema) = LOWER('{target_schema}') 
        AND LOWER(target_table) = LOWER('{target_table}')
        """

        log.info(f"Last Sync SQL: {sql}")

        conn = pg_hook.get_conn()
        cur = conn.cursor()
        cur.execute(sql)
        row = cur.fetchone()
        cur.close()

        time_string = "1900-01-01 01:01:01.000000+00:00"
        deafult_ts = datetime.fromisoformat(time_string)

        if row:
            return {
            "last_synced_at": row[0]
        }
        else:
            return {
            "last_synced_at": deafult_ts
            }

    except Exception as e:
        log.error(f"ERROR: {e}", exc_info=True)
        return None


# Update the Silver Fact Tables
def update_last_sync_silver_fact_table(pg_hook: PostgresHook, target_schema: str, target_table: str, last_synced_at: str) -> bool:

    try:
        sql = f"""
        UPDATE etl.etl_stats_silver_facts SET last_synced_at='{last_synced_at}'
        WHERE LOWER(target_schema) = LOWER('{target_schema}') 
        AND LOWER(target_table) = LOWER('{target_table}');
        """

        log.info(f"SQL: {sql}")

        conn = pg_hook.get_conn()
        cur = conn.cursor()
        cur.execute(sql)
        cur.close()

        return True

    except Exception as e:
        log.error(f"ERROR: {e}", exc_info=True)
        return False


    
# -------------------------------------------------------------------------
# Refresh Materialized Views
# -------------------------------------------------------------------------
def refresh_materialized_view (pg_hook: PostgresHook, mv_refresh_sql:str) -> int:

    try:
 
        log.info(f"MV Start: {mv_refresh_sql}")

        conn = pg_hook.get_conn()
        cur = conn.cursor()
        cur.execute(mv_refresh_sql)
        conn.commit()
        cur.close()

        log.info(f"MV End: {mv_refresh_sql}")

        return True

    except Exception as e:
        log.error(f"ERROR: {e}", exc_info=True)
        return None
    
# -------------------------------------------------------------------------
# Execute Functions
# -------------------------------------------------------------------------
def execute_function (pg_hook: PostgresHook, function_call_sql:str) -> int:

    try:
 
        log.info(f"Function Exec Start: {function_call_sql}")

        conn = pg_hook.get_conn()
        cur = conn.cursor()
        cur.execute(function_call_sql)
        row = cur.fetchone()
        conn.commit()
        cur.close()

        log.info(f"Function Exec End: {function_call_sql}")

        return int(row[0])

    except Exception as e:
        log.error(f"ERROR: {e}", exc_info=True)
        return 0
    
def upsert_etl_stats(pg_hook: PostgresHook, mapping: dict, **kwargs):
    try:
        conn = pg_hook.get_conn()
        cur = conn.cursor()

        # -------- DEFAULT VALUES FOR FIRST SYNC ----------
        default_initial_ts = datetime(1900, 1, 1, 1, 1, 0)
        default_zero_count = 0
        default_error = ""

        etl_status = kwargs.get("status", "RUNNING")


        sql = f"""
        INSERT INTO etl.etl_stats (
        table_name,
        table_no,
        source_conn_id,
        source_db_name,
        source_schema,
        source_table,
        target_conn_id,
        target_db_name,
        target_schema,
        target_table,
        pk_column,
        ts_column,
        last_synced_at,
        last_source_max_updated,
        last_source_row_count,
        last_target_row_count,
        status,
        last_error
    )
    VALUES (
        %s, %s, %s, %s, %s, %s, %s, %s, %s,
        %s, %s, %s, %s, %s, %s, %s, %s, %s
    )
    ON CONFLICT (target_db_name, target_schema, target_table)
    DO UPDATE SET
        status = COALESCE(EXCLUDED.status, '{default_error}'),
        last_error = COALESCE(EXCLUDED.last_error, '{default_error}'),
        updated_at = now()
        """

        table_name = f"{mapping['target_schema']}.{mapping['target_table']}"
        params = ( )

        if (etl_status != 'FAILED'):
            sql = sql +f""",last_synced_at = COALESCE(EXCLUDED.last_synced_at, '{default_initial_ts}'),
                last_source_max_updated = COALESCE(EXCLUDED.last_source_max_updated, '{default_initial_ts}'),
                last_source_row_count = COALESCE(EXCLUDED.last_source_row_count, 0),
                last_target_row_count = COALESCE(EXCLUDED.last_target_row_count, 0)
            """
            
            # Parameters
            params = (
                table_name,
                mapping['table_no'],
                mapping['source_conn_id'],
                mapping['source_db_name'],
                mapping['source_schema'],
                mapping['source_table'],

                mapping['target_conn_id'],
                mapping['target_db_name'],
                mapping['target_schema'],
                mapping['target_table'],

                mapping['pk_column'],
                mapping['ts_column'],

                kwargs.get("last_synced_at") or default_initial_ts,
                kwargs.get("last_source_max_updated") or default_initial_ts,
                kwargs.get("last_source_row_count", default_zero_count),
                kwargs.get("last_target_row_count", default_zero_count),
                kwargs.get("status", "RUNNING"),
                kwargs.get("last_error", default_error),
            )
        else:
            # Parameters
            params = (
                table_name,
                mapping['table_no'],
                mapping['source_conn_id'],
                mapping['source_db_name'],
                mapping['source_schema'],
                mapping['source_table'],

                mapping['target_conn_id'],
                mapping['target_db_name'],
                mapping['target_schema'],
                mapping['target_table'],

                mapping['pk_column'],
                mapping['ts_column'],

                kwargs.get("status", "RUNNING"),
                kwargs.get("last_error", default_error),
            )

        log.info(f"sql: {sql}")

        cur.execute(sql, params)
        conn.commit()
        cur.close()

        return True
    
    except Exception as e:
        log.error(f"ERROR: {e}", exc_info=True)
        return None


# -------------------------------------------------------------------------
# Source Metadata
# -------------------------------------------------------------------------
def get_source_max_updated(pg_hook: PostgresHook, mapping: dict):
    try:
        sql = f"""
        SELECT MAX({mapping['ts_column']})
        FROM {mapping['source_schema']}.{mapping['source_table']}
        """

        conn = pg_hook.get_conn()
        cur = conn.cursor()
        cur.execute(sql)
        row = cur.fetchone()
        cur.close()

        return row[0] if row else None
    except Exception as e:
        log.error(f"ERROR: {e}", exc_info=True)
        return None
    
def get_source_max_updated_by_timestamp(pg_hook: PostgresHook, mapping: dict):
    try:
        sql = f"""
        SELECT MAX({mapping['ts_column']})
        FROM {mapping['source_schema']}.{mapping['source_table']} 
        WHERE {mapping['ts_column']} > '{mapping['last_synced_at']}'::timestamp with time zone
        """

        conn = pg_hook.get_conn()
        cur = conn.cursor()
        cur.execute(sql)
        row = cur.fetchone()
        cur.close()

        return row[0] if row else None
    except Exception as e:
        log.error(f"ERROR: {e}", exc_info=True)
        return None
    
def get_source_row_count(pg_hook: PostgresHook, mapping: dict) -> int:
    try:
        sql = f"""
        SELECT count(1) AS total_rows
        FROM {mapping['source_schema']}.{mapping['source_table']} 
        """

        conn = pg_hook.get_conn()
        cur = conn.cursor()
        cur.execute(sql)
        row = cur.fetchone()
        cur.close()

        return int(row[0])
    
    except Exception as e:
        log.error(f"ERROR: {e}", exc_info=True)
        return None
    
def get_source_row_count_by_timestamp(pg_hook: PostgresHook, mapping: dict, last_synced_at) -> int:
    
    try:
        sql = f"""
        SELECT COUNT(1)
        FROM {mapping['source_schema']}.{mapping['source_table']} 
        WHERE {mapping['ts_column']} > '{last_synced_at}'::timestamp with time zone
        """

        log.info(f"sql: {sql}")

        conn = pg_hook.get_conn()
        cur = conn.cursor()
        cur.execute(sql)
        row = cur.fetchone()
        cur.close()

        return int(row[0])
    
    except Exception as e:
        log.error(f"ERROR: {e}", exc_info=True)
        return None
    
def get_target_row_count(pg_hook: PostgresHook, mapping: dict, last_synced_at) -> tuple[int, bool]:
    try:
        sql = f"""
        SELECT count(1) AS total_rows
        FROM {mapping['target_schema']}.{mapping['target_table']} 
        WHERE {mapping['ts_column']} > '{last_synced_at}'::timestamp with time zone
        """

        conn = pg_hook.get_conn()
        cur = conn.cursor()
        cur.execute(sql)
        row = cur.fetchone()
        cur.close()
        
        return int(row[0]), True
    
    except Exception as e:
        log.error(f"ERROR: {e}", exc_info=True)
        return 0, None
# -------------------------------------------------------------------------
# Schema Introspection & Sync
# -------------------------------------------------------------------------
def list_source_columns(pg_hook: PostgresHook, mapping: dict):
    try:
        sql = """
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
        """

        conn = pg_hook.get_conn()
        cur = conn.cursor()
        cur.execute(sql, (mapping["source_schema"], mapping["source_table"]))
        rows = cur.fetchall()
        cur.close()
        return rows
    
    except Exception as e:
        log.error(f"ERROR: {e}", exc_info=True)
        return None
    
def get_source_columns_with_details(hook: PostgresHook, mapping: dict) -> List[dict]:
    try:
        sql = """
        SELECT column_name, data_type, column_default, is_nullable,
            character_maximum_length, numeric_precision, numeric_scale
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        ORDER BY ordinal_position
        """

        conn = hook.get_conn()
        cur = conn.cursor()
        cur.execute(sql, (mapping["source_schema"], mapping["source_table"]))
        rows = cur.fetchall()
        cur.close()

        return [
            {
                "column_name": r[0],
                "data_type": r[1],
                "column_default": r[2],
                "is_nullable": r[3],
                "char_max_length": r[4],
                "numeric_precision": r[5],
                "numeric_scale": r[6]
            }
            for r in rows
        ]
    
    except Exception as e:
        log.error(f"ERROR: {e}", exc_info=True)
        return None
# ---------------------------------------------------------
# GET PRIMARY KEY COLUMNS (SUPPORT COMPOSITE PK)
# ---------------------------------------------------------
def get_primary_key_columns(hook: PostgresHook, mapping: dict) -> List[str]:
    try:
        sql = """
        SELECT kcu.column_name
        FROM information_schema.table_constraints tc
        JOIN information_schema.key_column_usage kcu
        ON kcu.constraint_name = tc.constraint_name
        AND kcu.constraint_schema = tc.constraint_schema
        WHERE tc.constraint_type = 'PRIMARY KEY'
        AND tc.table_catalog = %s
        AND tc.table_schema = %s
        AND tc.table_name  = %s
        ORDER BY kcu.ordinal_position
        """

        conn = hook.get_conn()
        cur = conn.cursor()
        cur.execute(sql, (mapping["source_db_name"], mapping["source_schema"], mapping["source_table"]))
        rows = cur.fetchall()
        cur.close()

        return [r[0] for r in rows]
    
    except Exception as e:
        log.error(f"ERROR: {e}", exc_info=True)
        return None
    
# ---------------------------------------------------------
# TYPE MAPPING + SERIAL NORMALIZATION
# ---------------------------------------------------------
def map_column_type(col: dict, is_pk: bool) -> str:
    try:
        dt = col["data_type"].lower()
        col_default = col["column_default"]
        is_serial = col_default and "nextval" in str(col_default).lower()

        # PK: serial → integer | bigserial → bigint
        if is_pk and is_serial:
            if "big" in dt:
                return "BIGINT"
            return "INTEGER"

        # varchar
        if dt in ("character varying", "varchar"):
            length = col["char_max_length"]
            return f"VARCHAR({length})" if length else "TEXT"

        # numeric
        if dt == "numeric":
            p = col["numeric_precision"]
            s = col["numeric_scale"]
            return f"NUMERIC({p},{s})" if p else "NUMERIC"

        # simple maps
        if dt in ("integer", "int4"):
            return "INTEGER"
        elif dt in ("bigint", "int8"):
            return "BIGINT"
        elif dt in ("smallint", "int2"):
            return "SMALLINT"
        elif "timestamp with time zone" in dt:
            return "TIMESTAMP WITH TIME ZONE"
        elif "timestamp" in dt:
            return "TIMESTAMP"
        elif dt == "boolean":
            return "BOOLEAN"
        elif "jsonb" in dt:
            return "JSONB"
        elif "json" in dt:
            return "JSON"
       
        return col["data_type"].upper()
    
    except Exception as e:
        log.error(f"ERROR: {e}", exc_info=True)
        return None
    
# ---------------------------------------------------------
# GENERATE CREATE TABLE SQL (OPTION A)
# ---------------------------------------------------------
def generate_create_table_sql(mapping: dict, cols: List[dict], pk_cols: List[str]) -> str:

    try:
        col_defs = []

        for col in cols:
            name = col["column_name"]
            typ = map_column_type(col, name in pk_cols)
            not_null = " NOT NULL" if col["is_nullable"] == "NO" else ""
            col_defs.append(f'"{name}" {typ} {not_null}')

        pk_clause = ""
        if pk_cols:
            pk_clause = f""", PRIMARY KEY ("{pk_cols}") """
            # pk_clause = ", PRIMARY KEY (" + ", ".join(f'"{c}"' for c in pk_cols) + ")"

        return f"""
    CREATE TABLE {mapping['target_schema']}.{mapping['target_table']} (
        {", ".join(col_defs)}
        {pk_clause}
    );
    """.strip()
    
    except Exception as e:
        log.error(f"ERROR: {e}", exc_info=True)
        return None

# ---------------------------------------------------------
# CREATE INDEX on updated_at Column
# ---------------------------------------------------------
def create_index_on_ts_column(hook: PostgresHook, target_schema: str, target_table: str, ts_column: str) -> bool:

    try:

        index_name = f"indx_{target_table}_{ts_column}"

        CHECK_INDEX_SQL = f"""
        SELECT 1
        FROM pg_indexes
        WHERE schemaname = '{target_schema}'
        AND tablename = '{target_table}'
        AND indexname = '{index_name}';
        """

        CREATE_INDEX_SQL = f"""
        CREATE INDEX IF NOT EXISTS {index_name} 
        ON {target_schema}.{target_table} ({ts_column});
        """
        
        conn = hook.get_conn()
        cur = conn.cursor()
        cur.execute(CHECK_INDEX_SQL)

        index_exists = cur.fetchone()

        if not index_exists:
            cur.execute(CREATE_INDEX_SQL)
            conn.commit()
            cur.close()

        return True
    
    except Exception as e:
        log.error(f"ERROR: {e}", exc_info=True)
        return None


# -----------------------
# Target introspection
# -----------------------
def list_target_columns(hook: PostgresHook, mapping: dict) -> List[str]:
    try:
        sql = """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = %s AND table_name = %s
        """
        log.info(f"Target columns sql: {sql}")
        conn = hook.get_conn()
        cur = conn.cursor()
        cur.execute(sql, (mapping["target_schema"], mapping["target_table"]))
        rows = [r[0] for r in cur.fetchall()]
        cur.close()
        return rows

    except Exception as e:
        log.error(f"ERROR: {e}", exc_info=True)
        return None
    
# -----------------------
# Target Schema Creation
# -----------------------

def ensure_target_schema_exists(target_hook: PostgresHook, mapping: dict):
    try:
        """
        Create target schema if it does not exist.
        """
        schema = mapping["target_schema"]
        conn = target_hook.get_conn()
        cur = conn.cursor()

        sql = f"CREATE SCHEMA IF NOT EXISTS {schema};"
        log.info(f"Ensuring target schema exists: {sql}")
        cur.execute(sql)

        conn.commit()
        cur.close()

        return True
    
    except Exception as e:
        log.error(f"ERROR: {e}", exc_info=True)
        return None
    
# ---------------------------------------------------------
# ENSURE TARGET TABLE EXISTS + SYNC MISSING COLUMNS
# ---------------------------------------------------------
def ensure_target_table_exists_and_add_missing_columns(target_hook: PostgresHook, mapping: dict, cols: List[dict], pk_cols: List[str]):
    try:
        conn = target_hook.get_conn()
        cur = conn.cursor()

        # Does table exist?
        cur.execute("""
            SELECT 1
            FROM information_schema.tables
            WHERE table_schema=%s AND table_name=%s
        """, (mapping["target_schema"], mapping["target_table"]))

        exists = cur.fetchone()

        pk_cols = mapping["pk_column"]

        if not exists:
            # Create Table
            create_sql = generate_create_table_sql(mapping, cols, pk_cols)
            log.info(f"Creating target table:\n{create_sql}")
            cur.execute(create_sql)
            conn.commit()

            # Create Index on ts column
            status = create_index_on_ts_column(hook=target_hook, target_schema=mapping["target_schema"], target_table=mapping["target_table"], ts_column=mapping["ts_column"])

            if status is None:
                raise Exception(f"Unable to create index on {mapping["target_table"]} table !!!")
            
        else:
            # Add missing columns only
            existing = set(list_target_columns(target_hook, mapping))

            for col in cols:
                name = col["column_name"]
                if name not in existing:
                    typ = map_column_type(col, name in pk_cols)
                    not_null = " NOT NULL" if col["is_nullable"] == "NO" else ""
                    alter = f"""
                    ALTER TABLE {mapping['target_schema']}.{mapping['target_table']}
                    ADD COLUMN "{name}" {typ} {not_null};
                    """
                    log.info(f"Adding missing column: {alter}")
                    cur.execute(alter)
                    conn.commit()

        cur.close()

        return True
    
    except Exception as e:
        log.error(f"ERROR: {e}", exc_info=True)
        return None
    

# ---------------------------------------------------------
# FETCH CHANGED ROWS (GENERATOR)
# ---------------------------------------------------------
def fetch_changed_rows(source_hook: PostgresHook, mapping: dict, since_ts=None, chunk_size=DEFAULT_CHUNK_SIZE):
    try:
        conn = source_hook.get_conn()
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

        upd = mapping["ts_column"]
        pk = mapping["pk_column"]

        log.info(f"DEBUG TS_COLUMN TYPE: {type(mapping['ts_column'])}, VALUE={mapping['ts_column']}, LAST_UPDATED_AT={since_ts}")

        # -----------------------------
        # SAFELY COUNT ROWS
        # -----------------------------
        if since_ts:
            sql = f"""
                SELECT COUNT(1) AS total
                FROM {mapping['source_schema']}.{mapping['source_table']}
                WHERE {upd} > '{since_ts}'::timestamp with time zone
            """
            cur.execute(sql)
        else:
            sql = f"""
                SELECT COUNT(1) AS total
                FROM {mapping['source_schema']}.{mapping['source_table']}
            """
            cur.execute(sql)

        row = cur.fetchone()          # ← Only one call

        # RealDictCursor → {"total": N}
        # Default cursor → (N,)
        if row is None:
            total = 0
        elif isinstance(row, dict):
            total = row.get("total", 0)
        else:
            total = row[0]

        if total == 0:
            cur.close()
            return

        # -----------------------------
        # CHUNKED FETCH
        # -----------------------------
        pages = math.ceil(total / float(chunk_size))
        offset = 0

        for _ in range(pages):
            if since_ts:
                sql = f"""
                    SELECT *
                    FROM {mapping['source_schema']}.{mapping['source_table']}
                    WHERE {upd} > '{since_ts}'::timestamp with time zone
                    ORDER BY {upd}
                    LIMIT {chunk_size} OFFSET {offset}
                """
                cur.execute(sql)

            else:
                sql = f"""
                    SELECT *
                    FROM {mapping['source_schema']}.{mapping['source_table']}
                    ORDER BY {pk}
                    LIMIT {chunk_size} OFFSET {offset}
                """
                cur.execute(sql)

            rows = cur.fetchall()
            if not rows:
                break

            yield rows
            offset += len(rows)

        cur.close()

    except Exception as e:
        log.error(f"ERROR: {e}", exc_info=True)
        return None
    
# ---------------------------------------------------------
# UPSERT INTO TARGET
# ---------------------------------------------------------
def upsert_rows_to_target(target_hook: PostgresHook, mapping: dict, rows: List[dict]) -> tuple[int, bool]:
    """
    Insert or update rows into target table.
    rows = list[dict] fetched from source using RealDictCursor
    """
    try:
        if not rows:
            return 0

        conn = target_hook.get_conn()
        cur = conn.cursor()

        target_schema = mapping["target_schema"]
        target_table = mapping["target_table"]
        pk = mapping["pk_column"]

        # ------------------------------------------------------------
        # Fetch column list in correct order from target table
        # ------------------------------------------------------------
        cur.execute(
            """
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = %s AND table_name = %s
            ORDER BY ordinal_position
            """,
            (target_schema, target_table),
        )

        dtype_rows = cur.fetchall()
        col_types = {name: dtype for name, dtype in dtype_rows}

        json_columns = {
            col for col, dtype in col_types.items()
            if dtype.lower() in ("json", "jsonb")
        }

        ordered_cols = [row[0] for row in dtype_rows]

        # ------------------------------------------------------------
        # Convert dict rows → tuples ordered as per target table schema
        # ------------------------------------------------------------
        values = []
        for r in rows:
            cleaned_row = []
            for col in ordered_cols:
                v = r.get(col)

                if isinstance(v, dict):
                    v = json.dumps(v)
                # Convert list/tuple → JSON (only for JSONB columns)
                elif isinstance(v, (list, tuple)):
                    if col in json_columns:
                        v = json.dumps(v)

                cleaned_row.append(v)

            values.append(tuple(cleaned_row))

        # values = []
        # for r in rows:
        #     values.append(tuple(r.get(col) for col in ordered_cols))

        # ------------------------------------------------------------
        # Prepare SQL
        # ------------------------------------------------------------
        col_sql = ", ".join([f'"{c}"' for c in ordered_cols])

        update_sets = ", ".join(
            [f'"{c}" = EXCLUDED."{c}"' for c in ordered_cols if c != pk]
        )

        sql = f"""
        INSERT INTO {target_schema}.{target_table} ({col_sql})
        VALUES %s
        ON CONFLICT ("{pk}") DO UPDATE SET
        {update_sets}
        """

        # ------------------------------------------------------------
        # Execute bulk insert / update
        # ------------------------------------------------------------
        psycopg2.extras.execute_values(
            cur,
            sql,
            values,
            template=None,
            page_size=100
        )

        conn.commit()

        affected = cur.rowcount

        # log.info(f"Affected Rows of {target_table}: {affected}")

        cur.close()
        return affected, True

    except Exception as e:
        log.error(f"ERROR: {e}", exc_info=True)
        return 0, None

