"""
metadata_fetcher.py
───────────────────
Job lấy metadata (tên cột + kiểu dữ liệu) từ source database (Oracle/MySQL)
rồi lưu vào cột source_schema trong bảng topic_configurations.

Luồng hoạt động:
  1. Đọc bảng topic_configurations (có thể filter theo table_source).
  2. Với mỗi bản ghi, dùng system_name để lấy thông tin connection.
  3. Kết nối tới source DB, query metadata của bảng schema_name.table_source.
  4. Lưu kết quả metadata (JSON) vào cột source_schema của topic_configurations.

Cách chạy:
  python -m core.metadata_fetcher                         # Xử lý tất cả
  python -m core.metadata_fetcher --table_source MY_TABLE # Chỉ xử lý 1 bảng
"""

import argparse
import json
import logging
import os
import sys
from typing import Dict, List, Optional

from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
logger = logging.getLogger("metadata_fetcher")

# ──────────────────────────────────────────────
# Control DB helpers  (PostgreSQL – dùng psycopg2)
# ──────────────────────────────────────────────

def _get_control_db_connection():
    """
    Tạo connection tới Control DB (PostgreSQL) dựa trên biến môi trường.
    """
    try:
        import psycopg2
    except ImportError:
        raise ImportError(
            "psycopg2 chưa được cài đặt. "
            "Chạy: pip install psycopg2-binary"
        )

    host = os.getenv("DB_HOST", "localhost")
    port = int(os.getenv("DB_PORT", "5432"))
    dbname = os.getenv("DB_SERVICE_NAME", "metadata")
    user = os.getenv("DB_USER", "iceberg")
    password = os.getenv("DB_PASSWORD", "")

    conn = psycopg2.connect(
        host=host,
        port=port,
        dbname=dbname,
        user=user,
        password=password,
    )
    return conn


def get_topic_configurations(
    table_source: Optional[str] = None,
) -> List[Dict]:
    """
    Đọc bảng topic_configurations.
    Nếu table_source được cung cấp → thêm WHERE table_source = :val.
    Trả về list[dict] với keys: schema_name, system_name, table_source, source_type.
    """
    conn = _get_control_db_connection()
    try:
        cur = conn.cursor()

        sql = """
            SELECT schema_name,
                   system_name,
                   table_source,
                   COALESCE(source_type, 'oracle') AS source_type
            FROM topic_configurations
        """
        params: tuple = ()

        if table_source:
            sql += " WHERE table_source = %s"
            params = (table_source,)

        cur.execute(sql, params)
        columns = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
        return [dict(zip(columns, row)) for row in rows]
    finally:
        conn.close()


# ──────────────────────────────────────────────
# Source DB connection helpers
# ──────────────────────────────────────────────

def _get_source_connection_info(system_name: str) -> Dict[str, str]:
    """
    Lấy thông tin connection cho source DB dựa trên system_name.

    Quy ước biến môi trường:
        {SYSTEM_NAME}_JDBC_URL      – JDBC URL hoặc connection string
        {SYSTEM_NAME}_JDBC_USER     – Username
        {SYSTEM_NAME}_JDBC_PASSWORD – Password

    Ví dụ với system_name = "ERP":
        ERP_JDBC_URL=jdbc:oracle:thin:@//host:1521/service
        ERP_JDBC_USER=admin
        ERP_JDBC_PASSWORD=secret
    """
    prefix = system_name.upper().replace("-", "_").replace(" ", "_")

    jdbc_url = os.getenv(f"{prefix}_JDBC_URL", "")
    jdbc_user = os.getenv(f"{prefix}_JDBC_USER", "")
    jdbc_password = os.getenv(f"{prefix}_JDBC_PASSWORD", "")

    if not jdbc_url:
        raise ValueError(
            f"Không tìm thấy biến môi trường {prefix}_JDBC_URL "
            f"cho system_name='{system_name}'"
        )

    return {
        "jdbc_url": jdbc_url,
        "jdbc_user": jdbc_user,
        "jdbc_password": jdbc_password,
    }


# ──────────────────────────────────────────────
# Fetch metadata từ source DB
# ──────────────────────────────────────────────

def _fetch_oracle_metadata(conn_info: Dict, schema_name: str, table_name: str) -> Dict[str, str]:
    """
    Query ALL_TAB_COLUMNS trên Oracle để lấy metadata.
    Trả về dict {column_name: full_data_type}.
    """
    try:
        import oracledb
    except ImportError:
        raise ImportError(
            "oracledb chưa được cài đặt. "
            "Chạy: pip install oracledb"
        )

    # Parse JDBC URL → host, port, service
    jdbc_url = conn_info["jdbc_url"]
    dsn = _parse_oracle_jdbc_url(jdbc_url)

    connection = oracledb.connect(
        user=conn_info["jdbc_user"],
        password=conn_info["jdbc_password"],
        dsn=dsn,
    )

    try:
        cursor = connection.cursor()
        query = """
            SELECT COLUMN_NAME,
                   CASE
                       WHEN DATA_TYPE = 'NUMBER' THEN
                           CASE
                               WHEN DATA_PRECISION IS NULL THEN 'NUMBER'
                               WHEN DATA_SCALE IS NULL OR DATA_SCALE = 0
                                   THEN 'NUMBER(' || DATA_PRECISION || ')'
                               ELSE 'NUMBER(' || DATA_PRECISION || ',' || DATA_SCALE || ')'
                           END
                       WHEN DATA_TYPE IN ('VARCHAR2', 'NVARCHAR2', 'CHAR', 'NCHAR')
                           THEN DATA_TYPE || '(' || DATA_LENGTH || ')'
                       ELSE DATA_TYPE
                   END AS FULL_DATA_TYPE
            FROM ALL_TAB_COLUMNS
            WHERE OWNER = :owner
              AND TABLE_NAME = :tbl
            ORDER BY COLUMN_ID
        """
        cursor.execute(query, {"owner": schema_name.upper(), "tbl": table_name.upper()})
        rows = cursor.fetchall()

        if not rows:
            logger.warning(
                f"Không tìm thấy metadata cho {schema_name}.{table_name} trên Oracle"
            )
            return {}

        return {row[0]: row[1] for row in rows}
    finally:
        connection.close()


def _fetch_mysql_metadata(conn_info: Dict, schema_name: str, table_name: str) -> Dict[str, str]:
    """
    Query information_schema.columns trên MySQL để lấy metadata.
    Trả về dict {column_name: column_type}.
    """
    try:
        import pymysql
    except ImportError:
        raise ImportError(
            "pymysql chưa được cài đặt. "
            "Chạy: pip install pymysql"
        )

    # Parse JDBC URL → host, port, database
    jdbc_url = conn_info["jdbc_url"]
    host, port, _ = _parse_mysql_jdbc_url(jdbc_url)

    connection = pymysql.connect(
        host=host,
        port=port,
        user=conn_info["jdbc_user"],
        password=conn_info["jdbc_password"],
        database=schema_name,
        charset="utf8mb4",
    )

    try:
        cursor = connection.cursor()
        query = """
            SELECT COLUMN_NAME, COLUMN_TYPE
            FROM information_schema.columns
            WHERE TABLE_SCHEMA = %s
              AND TABLE_NAME = %s
            ORDER BY ORDINAL_POSITION
        """
        cursor.execute(query, (schema_name, table_name))
        rows = cursor.fetchall()

        if not rows:
            logger.warning(
                f"Không tìm thấy metadata cho {schema_name}.{table_name} trên MySQL"
            )
            return {}

        return {row[0]: row[1] for row in rows}
    finally:
        connection.close()


def fetch_table_metadata(
    source_type: str,
    conn_info: Dict,
    schema_name: str,
    table_name: str,
) -> Dict[str, str]:
    """
    Factory method – gọi hàm fetch tương ứng với source_type.
    Trả về dict {column_name: data_type_string}.
    """
    source_type_lower = source_type.lower().strip()
    if source_type_lower == "oracle":
        return _fetch_oracle_metadata(conn_info, schema_name, table_name)
    elif source_type_lower == "mysql":
        return _fetch_mysql_metadata(conn_info, schema_name, table_name)
    else:
        raise ValueError(f"source_type không được hỗ trợ: {source_type}")


# ──────────────────────────────────────────────
# Lưu source_schema vào topic_configurations
# ──────────────────────────────────────────────

def update_source_schema(
    schema_name: str,
    table_source: str,
    source_schema_json: str,
) -> None:
    """
    UPDATE topic_configurations
    SET source_schema = :json
    WHERE schema_name = :schema AND table_source = :table
    """
    conn = _get_control_db_connection()
    try:
        cur = conn.cursor()
        cur.execute(
            """
            UPDATE topic_configurations
            SET source_schema = %s
            WHERE schema_name = %s
              AND table_source = %s
            """,
            (source_schema_json, schema_name, table_source),
        )
        conn.commit()
        logger.info(
            f"✓ Đã cập nhật source_schema cho {schema_name}.{table_source} "
            f"({cur.rowcount} row(s) affected)"
        )
    finally:
        conn.close()


# ──────────────────────────────────────────────
# JDBC URL parsers
# ──────────────────────────────────────────────

def _parse_oracle_jdbc_url(jdbc_url: str) -> str:
    """
    Chuyển JDBC URL Oracle → DSN cho oracledb.
    Hỗ trợ các dạng:
      jdbc:oracle:thin:@//host:port/service_name
      jdbc:oracle:thin:@host:port:sid
      jdbc:oracle:thin:@host:port/service_name
    """
    import re

    url = jdbc_url.replace("jdbc:oracle:thin:@", "")

    # Dạng //host:port/service
    match = re.match(r"^//([^:]+):(\d+)/(.+)$", url)
    if match:
        return f"{match.group(1)}:{match.group(2)}/{match.group(3)}"

    # Dạng host:port/service
    match = re.match(r"^([^:]+):(\d+)/(.+)$", url)
    if match:
        return f"{match.group(1)}:{match.group(2)}/{match.group(3)}"

    # Dạng host:port:sid
    match = re.match(r"^([^:]+):(\d+):(.+)$", url)
    if match:
        return f"{match.group(1)}:{match.group(2)}/{match.group(3)}"

    # Fallback – trả về nguyên bản
    return url


def _parse_mysql_jdbc_url(jdbc_url: str) -> tuple:
    """
    Chuyển JDBC URL MySQL → (host, port, database).
    Dạng: jdbc:mysql://host:port/database
    """
    import re

    url = jdbc_url.replace("jdbc:mysql://", "")
    match = re.match(r"^([^:]+):(\d+)/([^?]+)", url)
    if match:
        return match.group(1), int(match.group(2)), match.group(3)

    # Fallback – không có port
    match = re.match(r"^([^/]+)/([^?]+)", url)
    if match:
        return match.group(1), 3306, match.group(2)

    raise ValueError(f"Không thể parse MySQL JDBC URL: {jdbc_url}")


# ──────────────────────────────────────────────
# Main entry point
# ──────────────────────────────────────────────

def run(table_source: Optional[str] = None) -> None:
    """
    Luồng chính:
      1. Đọc topic_configurations (filter theo table_source nếu có)
      2. Với mỗi bản ghi → lấy metadata từ source DB
      3. Lưu metadata JSON vào cột source_schema
    """
    logger.info("=" * 60)
    logger.info("BẮT ĐẦU JOB METADATA FETCHER")
    logger.info("=" * 60)

    if table_source:
        logger.info(f"Filter: table_source = '{table_source}'")
    else:
        logger.info("Xử lý tất cả bản ghi trong topic_configurations")

    # 1. Đọc cấu hình
    configs = get_topic_configurations(table_source)
    logger.info(f"Tìm thấy {len(configs)} bản ghi cần xử lý")

    if not configs:
        logger.warning("Không tìm thấy bản ghi nào. Kết thúc.")
        return

    success_count = 0
    error_count = 0

    for cfg in configs:
        schema_name = cfg["schema_name"]
        system_name = cfg["system_name"]
        tbl_source = cfg["table_source"]
        source_type = cfg["source_type"]

        logger.info(f"─── Xử lý: {schema_name}.{tbl_source} "
                     f"(system={system_name}, type={source_type})")

        try:
            # 2. Lấy thông tin connection
            conn_info = _get_source_connection_info(system_name)

            # 3. Fetch metadata
            metadata = fetch_table_metadata(
                source_type=source_type,
                conn_info=conn_info,
                schema_name=schema_name,
                table_name=tbl_source,
            )

            if not metadata:
                logger.warning(f"  ⚠ Không có metadata → bỏ qua")
                error_count += 1
                continue

            logger.info(f"  Lấy được {len(metadata)} cột")

            # 4. Chuyển thành JSON và lưu
            source_schema_json = json.dumps(metadata, ensure_ascii=False)
            update_source_schema(schema_name, tbl_source, source_schema_json)
            success_count += 1

        except Exception as e:
            logger.error(f"  ✗ Lỗi khi xử lý {schema_name}.{tbl_source}: {e}")
            error_count += 1

    logger.info("=" * 60)
    logger.info(f"KẾT THÚC: {success_count} thành công, {error_count} lỗi")
    logger.info("=" * 60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Fetch metadata từ source database và lưu vào topic_configurations"
    )
    parser.add_argument(
        "--table_source",
        type=str,
        default=None,
        help="Lọc theo table_source cụ thể (tùy chọn)",
    )
    args = parser.parse_args()
    run(table_source=args.table_source)
