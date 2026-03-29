import json
import json
import logging
from typing import Dict, List, Tuple
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, 
    IntegerType, LongType, DecimalType, DataType
)
import re
import os

logger = logging.getLogger(__name__)

def map_oracle_type_to_spark(oracle_type: str) -> DataType:
    """
    Oracle NUMBER(9,0)  → IntegerType()
    Oracle NUMBER(19,0) → LongType()
    Oracle NUMBER(p,s)  → DecimalType(p,s) nếu s > 0
    Oracle VARCHAR2(n)  → StringType()
    Oracle DATE         → TimestampType()
    Oracle TIMESTAMP    → TimestampType()
    Oracle CLOB/BLOB    → StringType()
    """
    ot = oracle_type.upper().strip()
    
    match_number = re.match(r"NUMBER\((\d+)(?:,(\d+))?\)", ot)
    if match_number:
        p = int(match_number.group(1))
        s = int(match_number.group(2)) if match_number.group(2) else 0
        if s == 0:
            if p <= 9:
                return IntegerType()
            elif p <= 18:
                return LongType()
            else:
                return DecimalType(p, 0)
        else:
            return DecimalType(p, s)
            
    if ot.startswith("NUMBER"):
        return DecimalType(38, 18)  # default for generic NUMBER
        
    if ot.startswith("VARCHAR") or ot.startswith("NVARCHAR") or ot.startswith("CHAR") or ot == "CLOB" or ot == "BLOB":
        return StringType()
        
    if ot.startswith("DATE") or ot.startswith("TIMESTAMP"):
        return TimestampType()
        
    # Default fallback
    return StringType()

def map_mysql_type_to_spark(mysql_type: str) -> DataType:
    """
    MySQL INT            → IntegerType()
    MySQL BIGINT         → LongType()
    MySQL DECIMAL(p,s)   → DecimalType(p,s)
    MySQL VARCHAR(n)     → StringType()
    MySQL DATETIME       → TimestampType()
    MySQL TEXT/LONGTEXT  → StringType()
    """
    mt = mysql_type.upper().strip()
    
    if mt.startswith("TINYINT") or mt.startswith("SMALLINT") or mt.startswith("MEDIUMINT") or mt.startswith("INT"):
        return IntegerType()
        
    if mt.startswith("BIGINT"):
        return LongType()
        
    match_decimal = re.match(r"(?:DECIMAL|NUMERIC)\((\d+)(?:,(\d+))?\)", mt)
    if match_decimal:
        p = int(match_decimal.group(1))
        s = int(match_decimal.group(2)) if match_decimal.group(2) else 0
        return DecimalType(p, s)
        
    if mt.startswith("VARCHAR") or mt.startswith("CHAR") or mt.endswith("TEXT") or mt.endswith("BLOB"):
        return StringType()
        
    if mt.startswith("DATE") or mt.startswith("TIME"):
        return TimestampType()
        
    # Default fallback
    return StringType()

class SchemaChangeReport:
    def __init__(self, added_cols: List[StructField], dropped_cols: List[StructField], 
                 type_changed_cols: List[Tuple[StructField, StructField]]):
        self.added_cols = added_cols
        self.dropped_cols = dropped_cols
        self.type_changed_cols = type_changed_cols
        
    @property
    def has_changes(self) -> bool:
        return bool(self.added_cols or self.dropped_cols or self.type_changed_cols)
        
    @property
    def needs_recreation(self) -> bool:
        """
        True nếu có type change không tương thích để Iceberg có thể thay đổi bằng ALTER TABLE.
        Trong Iceberg, int->long, float->double là an toàn, nhưng ngược lại hoặc string->int thì không.
        Để đơn giản, mọi type thay đổi đều được khuyến nghị recreate (trừ phi Iceberg support).
        """
        return bool(self.type_changed_cols)

class SchemaFetcher:
    """Lấy schema thực tế từ Config Database hoặc trực tiếp từ Source Database thay vì infer từ JSON"""
    def __init__(self, config: Dict):
        self.config = config
        
    def fetch_from_config(self) -> StructType:
        """
        Lấy schema từ cấu hình topic_config.
        Nếu có source_schema, sẽ parse và tạo schema.
        Nếu không có, fallback về schema infer được truyền vào.
        """
        source_type = str(self.config.get('source_type', ''))
        hints_str = self.config.get('source_schema')
        
        if not source_type or not hints_str:
            return None
            
        try:
            hints = json.loads(str(hints_str))
            fields = []
            for col_name, col_type in hints.items():
                if source_type.lower() == 'oracle':
                    spark_type = map_oracle_type_to_spark(col_type)
                elif source_type.lower() == 'mysql':
                    spark_type = map_mysql_type_to_spark(col_type)
                else:
                    spark_type = StringType()
                fields.append(StructField(col_name, spark_type, True))
            return StructType(fields)
        except Exception as e:
            logger.error(f"Error parsing source_schema: {e}")
            return None

    def fetch_from_source(self, spark) -> StructType:
        """
        Lấy schema thực tế từ source database (Oracle/MySQL) bằng Spark JDBC,
        thông qua query lấy type_name và columns.
        """
        jdbc_url = self.config.get('jdbc_url') or os.getenv('SOURCE_JDBC_URL')
        jdbc_user = self.config.get('jdbc_user') or os.getenv('SOURCE_JDBC_USER')
        jdbc_password = self.config.get('jdbc_password') or os.getenv('SOURCE_JDBC_PASSWORD')
        source_table = self.config.get('source_table_name') or self.config.get('table_name')
        source_type = str(self.config.get('source_type', '')).lower()
        
        if not jdbc_url or not source_table:
            logger.warning("Missing jdbc_url or source_table to fetch from source.")
            return None
            
        try:
            query = ""
            if source_type == 'oracle':
                query = f"""
                SELECT 
                    COLUMN_NAME,
                    CASE 
                        WHEN DATA_TYPE = 'NUMBER' THEN 
                            CASE 
                                WHEN DATA_PRECISION IS NULL THEN 'NUMBER'
                                WHEN DATA_SCALE IS NULL OR DATA_SCALE = 0 THEN 'NUMBER(' || DATA_PRECISION || ')'
                                ELSE 'NUMBER(' || DATA_PRECISION || ',' || DATA_SCALE || ')'
                            END
                        WHEN DATA_TYPE IN ('VARCHAR2', 'CHAR') THEN 
                            DATA_TYPE || '(' || DATA_LENGTH || ')'
                        ELSE DATA_TYPE 
                    END AS FULL_DATA_TYPE
                FROM ALL_TAB_COLUMNS 
                WHERE TABLE_NAME = '{source_table.upper()}'
                ORDER BY COLUMN_ID
                """
            elif source_type == 'mysql':
                query = f"""
                (SELECT COLUMN_NAME, COLUMN_TYPE as DATA_TYPE 
                 FROM information_schema.columns 
                 WHERE table_name = '{source_table}') tmp
                """
            else:
                # Fallback to spark default read if no specific logic
                df = spark.read.format("jdbc") \
                    .option("url", jdbc_url) \
                    .option("dbtable", source_table) \
                    .option("user", jdbc_user) \
                    .option("password", jdbc_password) \
                    .load()
                return df.schema
                
            metadata_df = spark.read.format("jdbc") \
                .option("url", jdbc_url) \
                .option("dbtable", query) \
                .option("user", jdbc_user) \
                .option("password", jdbc_password) \
                .load()
                
            rows = metadata_df.collect()
            fields = []
            
            for row in rows:
                col_name = row['COLUMN_NAME']
                if source_type == 'oracle':
                    data_type = row['DATA_TYPE']
                    precision = row['DATA_PRECISION']
                    scale = row['DATA_SCALE']
                    if data_type.startswith("NUMBER") and precision is not None:
                         sc = scale if scale is not None else 0
                         type_str = f"NUMBER({int(precision)},{int(sc)})"
                    else:
                         type_str = data_type
                    spark_type = map_oracle_type_to_spark(type_str)
                else:
                    type_str = row['DATA_TYPE']
                    spark_type = map_mysql_type_to_spark(type_str)
                    
                fields.append(StructField(col_name, spark_type, True))
                
            return StructType(fields)
        except Exception as e:
            logger.error(f"Error fetching schema from source JDBC: {e}")
            return None

    def fetch(self, spark) -> StructType:
        """
        Lấy schema dựa vào config schema_fetch_mode.
        Giá trị: 'config' (mặc định) hoặc 'source'.
        """
        fetch_mode = str(self.config.get('schema_fetch_mode', 'config')).lower()
        if fetch_mode == 'source':
            logger.info("Fetching schema directly from source...")
            schema = self.fetch_from_source(spark)
            if schema is not None:
                return schema
            logger.warning("Fallback to config fetch mode because fetching from source failed or returned None.")
            
        logger.info("Fetching schema from config hints...")
        return self.fetch_from_config()

class SchemaChangeDetector:
    """So sánh actual schema với Silver table schema hiện tại"""
    
    def detect(self, current_silver_schema: StructType, actual_new_schema: StructType) -> SchemaChangeReport:
        if not current_silver_schema:
            return SchemaChangeReport([], [], [])
            
        current_fields = {f.name.lower(): f for f in current_silver_schema.fields}
        actual_fields = {f.name.lower(): f for f in actual_new_schema.fields}
        
        added_cols = []
        dropped_cols = []
        type_changed_cols = []
        
        for name, field in actual_new_schema.fields:
            if name.lower() not in current_fields:
                added_cols.append(field)
            else:
                curr_field = current_fields[name.lower()]
                if field.dataType != curr_field.dataType:
                    # Iceberg supports string types mapped as VARCHAR
                    # Ignore StringType if current is StringType or simpleString is similar
                    if isinstance(field.dataType, StringType) and isinstance(curr_field.dataType, StringType):
                        continue
                    if isinstance(field.dataType, DecimalType) and isinstance(curr_field.dataType, DecimalType):
                         if field.dataType.precision == curr_field.dataType.precision and field.dataType.scale == curr_field.dataType.scale:
                              continue
                         # Iceberg supports widening decimal, but for simplicity we flag it
                    type_changed_cols.append((curr_field, field))
                    
        for name, field in current_silver_schema.fields:
            if name.lower() not in actual_fields:
                dropped_cols.append(field)
                
        return SchemaChangeReport(added_cols, dropped_cols, type_changed_cols)

class SchemaEvolutionManager:
    """Áp dụng schema changes vào Silver Iceberg table"""
    
    def __init__(self, spark):
        self.spark = spark

    def apply(self, full_table_name: str, change_report: SchemaChangeReport, 
             strategy: str, allow_drop_column: bool, database: str, table_name: str,
             partition_cols: str, _spark_type_to_silver_ddl) -> bool:
        """
        Trả về True nếu cần tạo lại toàn bảng và chạy data type cast, False nếu chỉ ALTER an toàn.
        """
        if not change_report.has_changes:
            return False
            
        logger.info(f"Schema changes detected for {full_table_name}")
        
        if change_report.type_changed_cols:
            logger.warning(f"  - Type changes detected: {[f'{old.name}({old.dataType.simpleString()}->{new.dataType.simpleString()})' for old, new in change_report.type_changed_cols]}")
            
            if strategy == 'recreate':
                logger.warning(f"  - Recreating table {full_table_name} due to type changes.")
                return True
                
            logger.warning("  - Strategy is not 'recreate', skipping type changes.")
            
        for col in change_report.added_cols:
            ddl_type = _spark_type_to_silver_ddl(col.dataType)
            logger.info(f"  - Adding column: {col.name} {ddl_type}")
            try:
                self.spark.sql(f"ALTER TABLE {full_table_name} ADD COLUMNS ({col.name} {ddl_type})")
            except Exception as e:
                logger.error(f"  - Failed to add column {col.name}: {e}")
                
        for col in change_report.dropped_cols:
            if allow_drop_column:
                logger.info(f"  - Dropping column: {col.name}")
                try:
                    self.spark.sql(f"ALTER TABLE {full_table_name} DROP COLUMN {col.name}")
                except Exception as e:
                    logger.error(f"  - Failed to drop column {col.name}: {e}")
            else:
                logger.info(f"  - Skipping drop column: {col.name} (allow_drop_column=False)")
                
        return False
