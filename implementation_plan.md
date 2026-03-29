# Schema Evolution Handling cho Silver Layer

## Bối cảnh & Vấn đề

Hiện tại [silver_job.py](file:///e:/vnpt/starrock_streaming/core/silver_job.py) infer schema từ Bronze table mỗi lần chạy, nhưng khi source (Oracle/MySQL) thay đổi schema (thêm/xóa/đổi kiểu cột), Silver Iceberg table **không được cập nhật** → MERGE fail hoặc dữ liệu sai. Không có CDC nên không biết column nào thay đổi, phải so sánh toàn bộ.

**Key insight về MinIO:** Khi DROP Iceberg table, catalog chỉ xóa metadata (JSON files trong `.metadata/`). Parquet data files trên MinIO vẫn còn nguyên cho đến khi `VACUUM`/`expire_snapshots` chạy. → Ta có thể đọc lại dữ liệu cũ từ Silver chính nó để backfill sang Silver table mới.

---

## User Review Required

> [!IMPORTANT]
> **Chiến lược khi schema thay đổi — cần xác nhận:**
> 1. **ADD COLUMN**: An toàn — dùng `ALTER TABLE ADD COLUMN`. Cột mới = NULL cho data cũ.
> 2. **DROP COLUMN**: Xóa cột khỏi Silver table có thể mất data. Đề xuất: **chỉ `ALTER TABLE DROP COLUMN`** khi user xác nhận hoặc cấu hình `allow_drop_column = true`.
> 3. **TYPE CHANGE**: Đây là phức tạp nhất. Iceberg hỗ trợ widening (int→long, float→double), nhưng không hỗ trợ narrowing. Đề xuất: **recreate table** (CTAS + rename) nếu có type change không tương thích.

> [!WARNING]
> **Recreation table** (khi type change): Silver table cũ được đọc lại (Parquet files vẫn còn trên MinIO), cast sang schema mới, ghi vào bảng mới. **Không cần reset checkpoint, không cần replay từ Bronze.** Cần đủ bộ nhớ/tài nguyên để xử lý toàn bộ Silver data. Cấu hình qua env var `SILVER_SCHEMA_CHANGE_STRATEGY=recreate|alter_only`.

> [!NOTE]
> **Mapping type Oracle/MySQL → Iceberg** được viết dưới dạng hàm thuần Python, dễ chỉnh sửa không cần recompile. User có thể override mapping trong config database.

---

## Thiết kế tổng thể

### Luồng xử lý schema change và Cách xác định `new_schema`

Việc infer schema từ Bronze JSON data là **không đáng tin cậy** để detect schema change (vì JSON batch có thể thiếu field do null, hoặc data type bị Spark đoán sai, không phản ánh đúng kiểu thực tế như `NUMBER(19,2)` hay `VARCHAR2`). Do đó, `new_schema` (schema thực tế hiện tại của bảng nguồn) **cần phải lấy từ một Nguồn Sự Thật (Source of Truth) rõ ràng**, thông qua một trong hai cách:

1. **Direct Connection (Khuyến nghị nếu Silver job có quyền kết nối):** Job Silver dùng JDBC để kết nối thẳng tới source DB (Oracle/MySQL) và dùng các query metadata (như `SHOW CREATE TABLE`, `information_schema.columns`, hoặc `all_tab_columns`) để lấy chính xác danh sách cột và kiểu dữ liệu chuẩn đang có trên source.
2. **External Schema Sync Job (Khuyến nghị nếu không share JDBC account):** Dùng một job ngoài (ví dụ Cron/Airflow job hoặc một agent riêng) chuyên kết nối định kỳ tới source database để check schema. Nếu có thay đổi, job này save schema chuẩn vào một bảng cấu hình riêng trong Database (VD: `topic_schemas`). Job Silver khi chạy dòng stream sẽ đọc config table này để lấy `new_schema`.

Luồng chạy lúc này sẽ chuẩn xác và không còn mập mờ:
```
run() → fetch_actual_schema()         [Lấy trực tiếp từ JDBC source HOẶC từ Config Database]
     → map_source_type_to_iceberg()   [Dùng hàm map để convert Oracle/MySQL type sang Iceberg type]
     → detect_schema_changes()        [So sánh Actual Schema vs Silver Iceberg table hiện tại]
     ↓
     ADD COLUMN   → ALTER TABLE ADD COLUMN  (safe, data cũ = NULL)
     DROP COLUMN  → ALTER TABLE DROP COLUMN (cần thiết lập cờ confirm, biết chắc chắn bị drop do actual schema xóa)
     TYPE CHANGE  → Silver-to-Silver Recreation
     NO CHANGE    → create_silver_table_if_not_exists() [giữ nguyên]
```

### Silver-to-Silver Recreation Flow (TYPE CHANGE)

Khi type change không tương thích với Iceberg ALTER:

```
1. Đọc Silver table cũ → old_df (Parquet files vẫn còn trên MinIO)
2. Rename Silver table cũ → <table>_schema_bak_<timestamp>  
   (giữ Parquet files, chỉ đổi tên trong catalog)
3. CREATE TABLE <table_new> với schema mới
4. old_df.select(...cast to new types...) → writeTo(table_new).overwritePartitions()
5. Verify row count match
6. DROP TABLE <table>_schema_bak (hoặc giữ lại để audit)
7. Checkpoint KHÔNG BỊ RESET → stream tiếp tục từ offset sau cùng
   (data giữa recreation và stream tiếp theo sẽ được MERGE bình thường)
```

**Lợi ích so với Bronze replay:**
- ✅ Không reset checkpoint → không mất track Bronze offset
- ✅ Nhanh hơn nhiều (Silver data đã clean, không cần re-deduplicate)
- ✅ Không phụ thuộc vào Bronze snapshot còn sống hay không
- ⚠️ Cần Silver data chưa bị `expire_snapshots` xóa Parquet files

### Quyết định gộp hay tách
**Gộp chung trong [silver_job.py](file:///e:/vnpt/starrock_streaming/core/silver_job.py)** — lý do:
- Schema change chỉ xảy ra khi Silver job chạy (không phải continuous)
- Logic liên quan chặt đến Silver table lifecycle
- Tách riêng sẽ phải share SparkSession, phức tạp hơn
- Thêm một class `SchemaEvolutionManager` bên trong [silver_job.py](file:///e:/vnpt/starrock_streaming/core/silver_job.py) (hoặc `core/schema_evolution.py` để dễ test)

→ **Tách ra `core/schema_evolution.py`** nhưng được gọi từ [silver_job.py](file:///e:/vnpt/starrock_streaming/core/silver_job.py)

---

## Proposed Changes

### Component 1: Type Mapping Engine

#### [NEW] [schema_evolution.py](file:///e:/vnpt/starrock_streaming/core/schema_evolution.py)

File mới chứa toàn bộ logic schema evolution:

```python
# Các class/function chính:

def map_oracle_type_to_spark(oracle_type: str) -> DataType:
    """
    Oracle NUMBER(9,0)  → IntegerType
    Oracle NUMBER(19,0) → LongType  
    Oracle NUMBER(p,s)  → DecimalType(p,s) nếu s > 0
    Oracle VARCHAR2(n)  → StringType
    Oracle DATE         → TimestampType
    Oracle TIMESTAMP    → TimestampType
    Oracle CLOB/BLOB    → StringType
    """

def map_mysql_type_to_spark(mysql_type: str) -> DataType:
    """
    MySQL INT            → IntegerType
    MySQL BIGINT         → LongType
    MySQL DECIMAL(p,s)   → DecimalType(p,s)
    MySQL VARCHAR(n)     → StringType
    MySQL DATETIME       → TimestampType
    MySQL TEXT/LONGTEXT  → StringType
    """

class SchemaFetcher:
    """Lấy schema thực tế từ Source Database hoặc Config Database thay vì infer từ JSON"""
    def fetch_from_jdbc(self, jdbc_url, table_name) -> Dict[str, str]:
        """Dùng query all_tab_columns / information_schema để lấy type chuẩn gốc"""
        pass
        
    def fetch_from_config_db(self, topic_id) -> Dict[str, str]:
        """Dùng nếu dùng External Job lưu config schema vào CSDL tĩnh"""
        pass

class SchemaChangeDetector:
    """So sánh actual schema (đã map qua Spark types) với Silver table schema hiện tại"""
    
    def detect(self, current_silver_schema, actual_new_schema) -> SchemaChangeReport:
        """
        Returns:
          - added_cols: List[StructField]
          - dropped_cols: List[StructField]  
          - type_changed_cols: List[(StructField_old, StructField_new)]
          - has_changes: bool
          - needs_recreation: bool  # True nếu có type change không tương thích
        """

class SchemaEvolutionManager:
    """Áp dụng schema changes vào Silver Iceberg table"""

    def apply(self, silver_table, change_report, strategy) -> bool:
        """
        strategy = 'alter_only'   → chỉ ADD/DROP, bỏ qua type change
        strategy = 'recreate'     → DROP + CREATE + backfill từ Bronze
        
        Returns: True nếu cần chạy lại full stream (sau recreation)
        """
```

**Type mapping được lưu trong dict dễ override:**
```python
ORACLE_TYPE_MAP = {
    r"NUMBER\((\d+),0\)": lambda p: IntegerType() if int(p) <= 9 else LongType(),
    r"NUMBER\((\d+),(\d+)\)": lambda p, s: DecimalType(int(p), int(s)),
    r"VARCHAR2\(\d+\)": StringType(),
    "DATE": TimestampType(),
    ...
}
```

#### [MODIFY] [silver_job.py](file:///e:/vnpt/starrock_streaming/core/silver_job.py)

Thêm các thay đổi sau:

1. **Import `SchemaEvolutionManager`** từ `schema_evolution.py`

2. **Thêm method `_get_current_silver_schema()`** — đọc schema Iceberg hiện tại của Silver table (nếu đã tồn tại):
```python
def _get_current_silver_schema(self, silver_table: str) -> StructType | None
```

3. **Sửa [create_silver_table_if_not_exists()](file:///e:/vnpt/starrock_streaming/core/silver_job.py#274-321)** → tách ra `ensure_silver_table_schema()`:
```python
def ensure_silver_table_schema(self, database, table_name, new_schema, partition_cols):
    """
    - Nếu bảng chưa tồn tại → CREATE TABLE (giữ logic cũ)
    - Nếu bảng đã tồn tại → detect changes → apply changes
    """
```

4. **Thêm source_type hint** trong topic_config:
```python
# topic_config có thêm field:
# 'source_type': 'oracle' | 'mysql' | None  (None = dùng spark inference như cũ)
# 'source_schema': JSON string với mapping {col_name: source_ddl_type}
#   VD: '{"AMOUNT": "NUMBER(19,2)", "COUNT": "NUMBER(9,0)"}'
```

5. **Sửa [run()](file:///e:/vnpt/starrock_streaming/core/silver_job.py#612-691)** để gọi `ensure_silver_table_schema()` thay vì [create_silver_table_if_not_exists()](file:///e:/vnpt/starrock_streaming/core/silver_job.py#274-321)

### Component 2: Config database

Thêm column vào `topic_configurations` (không thuộc scope code này, nhưng schema_evolution cần đọc):

| Column | Type | Mô tả |
|---|---|---|
| `source_type` | VARCHAR | `oracle` / `mysql` / null |
| `source_schema` | TEXT | JSON: `{"COL": "NUMBER(9,0)"}` |
| `schema_change_strategy` | VARCHAR | `alter_only` / `recreate` (default: `alter_only`) |
| `allow_drop_column` | BOOLEAN | default: false |

> [!NOTE]
> Nếu chưa muốn thêm column vào DB, các giá trị này có thể đọc từ env var `SILVER_SCHEMA_CHANGE_STRATEGY`, `SILVER_ALLOW_DROP_COLUMN`.

---

## Những điểm quan trọng bạn có thể bỏ qua

> [!IMPORTANT]
> **1. Iceberg type widening có giới hạn:**
> Iceberg chỉ cho phép: `int→long`, `float→double`, `decimal(p,s)→decimal(p2,s)` với `p2>p`.
> Các thay đổi khác (string→int, int→decimal) cần recreate table.

> [!IMPORTANT]
> **2. Silver-to-Silver Recreation giữ nguyên checkpoint:**
> Dùng chiến lược rename cũ → copy với schema mới → drop cũ nên checkpoint **không bị ảnh hưởng**. Silver stream tiếp tục từ Bronze offset sau cùng bình thường. Data trong khoảng thời gian recreation (thường vài phút) sẽ được MERGE bình thường ở lần chạy tiếp theo.
>
> ⚠️ Rủi ro duy nhất: nếu Silver Parquet files đã bị `expire_snapshots` xóa trước khi recreation → fallback về Bronze replay + checkpoint reset. Nên để snapshot retention Silver ≥ 7 ngày để an toàn.

> [!IMPORTANT]
> **3. Không dùng Spark infer type từ JSON làm cơ sở check schema:**
> Việc dùng JSON inference của Spark sẽ khiến tất cả số thực thành `DoubleType`/`LongType`, bỏ lỡ các thông tin quan trọng như `NUMBER(19,0)`. Việc chúng ta xác định `new_schema` từ Nguồn Sự Thật trực tiếp (JDBC/Config Db) sẽ giải quyết triệt để lỗi kiểu dữ liệu và không phải cấu hình `source_schema` rườm rà bằng tay.

> [!IMPORTANT]
> **4. Vấn đề "Cột có bị xoá thật không" đã được giải quyết:**
> Nhờ vào việc so sánh trực tiếp với DDL schema hiện hữu tại Source Database (qua JDBC/Config Db), ta biết **chính xác 100% cột nào đã bị DELETE thực sự** ở nguồn. Không phải phỏng đoán vì data JSON đột ngột bị thiếu field như cũ nữa. Lưu ý: có thể vẫn cần user confirm việc DROP COLUMN này để đề phòng xóa Iceberg column nhầm.

> [!NOTE]
> **5. Partition columns không nên thay đổi:**
> Iceberg không cho phép ALTER TABLE CHANGE partition spec dễ dàng. Nếu partition column thay đổi type → cần recreate. Đây là edge case hiếm gặp, có thể log error và skip.

---


