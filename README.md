# Real Estate Project

Một pipeline data engineering thực tế để thu thập, xử lý và phân tích dữ liệu bất động sản Việt Nam từ nhadat247.com.vn.

## Mục lục

- [Kiến trúc dự án](#-kiến-trúc-dự-án)
- [Tính năng chính](#-tính-năng-chính)
- [Công nghệ sử dụng](#️-công-nghệ-sử-dụng)
- [Cài đặt và chạy](#-cài-đặt-và-chạy)
- [Sử dụng pipeline](#-sử-dụng-pipeline)
- [Data Exploration](#-data-exploration)
- [PostgreSQL Export](#️-postgresql-export)
- [Cấu trúc thư mục](#-cấu-trúc-thư-mục)

## Kiến trúc dự án

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Web Scraping  │ -> │   Data Process  │ -> │   Delta Lake    │ -> │   PostgreSQL    │ -> │   Data Explore  │
│  (Requests + BS)│    │   (Pandas)      │    │   (MinIO S3)    │    │   (Export)      │    │   (Jupyter)     │
└─────────────────┘    └─────────────────┘    └─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │                       │                       │
         └───────────────────────┼───────────────────────┼───────────────────────┼───────────────────────┘
                                 │                       │                       │
                    ┌─────────────────┐         ┌─────────────────┐    ┌─────────────────┐
                    │    Dagster     │         │    Analytics    │    │   SQL Queries   │
                    │ Orchestration  │         │   (DuckDB)      │    │  (PostgreSQL)   │
                    └─────────────────┘         └─────────────────┘    └─────────────────┘
```

### Luồng xử lý dữ liệu:
1. **Scraping Layer**: Thu thập dữ liệu từ nhadat247.com.vn sử dụng requests + BeautifulSoup, tránh DNS issues
2. **Processing Layer**: Xử lý dữ liệu với Pandas, chuẩn hóa format
3. **Storage Layer**: Lưu trữ ACID với Delta Lake trên MinIO S3-compatible
4. **Database Layer**: Tự động export từ Delta Lake sang PostgreSQL trong pipeline
5. **Analytics Layer**: Chạy SQL queries và Jupyter notebooks cho data exploration

**Key Data Flow**: Search Criteria → URL Generation → Threaded Requests Scraping → DataFrame Processing → Delta Lake Merge → PostgreSQL Export → SQL Analytics

## Tính năng chính

- ✅ **Web Scraping ổn định**: Sử dụng requests + BeautifulSoup, tránh DNS issues của Chrome/Selenium
- ✅ **ACID Transactions**: Delta Lake đảm bảo tính toàn vẹn dữ liệu
- ✅ **Schema Evolution**: Tự động adapt khi schema thay đổi
- ✅ **Cloud Storage**: MinIO S3-compatible cho storage agnostic
- ✅ **PostgreSQL Integration**: Pipeline tự động export từ Delta Lake sang PostgreSQL
- ✅ **Analytics Queries**: Script chuyên dụng để chạy SQL analytics trên PostgreSQL
- ✅ **Data Exploration**: Jupyter notebooks với DuckDB và PostgreSQL queries
- ✅ **Monitoring**: Dagster UI cho pipeline monitoring

## 🛠️ Công nghệ sử dụng

### Core Dependencies
- **Dagster 1.6.8**: Workflow orchestration và pipeline management
- **Dagster-DeltaLake-Pandas**: Delta Lake integration với Pandas
- **Dagster-Postgres**: PostgreSQL integration
- **Delta Lake**: ACID transactions và time travel cho data lake
- **PostgreSQL**: Relational database cho analytics và reporting
- **MinIO**: S3-compatible object storage
- **PyArrow**: Apache Arrow cho data processing
- **Pandas**: Data manipulation và analysis
- **DuckDB**: In-process analytical database (sử dụng trong notebooks)
- **SQLAlchemy**: ORM cho database operations
- **Requests**: HTTP client cho web scraping
- **BeautifulSoup4**: HTML parsing
- **Boto3**: AWS S3 API client (MinIO compatible)

### Development & Deployment
- **Dagstermill**: Jupyter notebook integration với Dagster
- **PostgreSQL Export Op**: Tích hợp export vào pipeline Dagster

## Cài đặt và chạy

### 1. Clone repository
```bash
git clone https://github.com/BinhTHB/Real-Estate_Project.git
cd Real-Estate_Project
```

### 2. Cài đặt dependencies
```bash
cd src/pipelines/real-estate

# Tạo virtual environment (nếu chưa có)
python -m venv .venv
# Kích hoạt virtual environment 
.\venv\Scripts\activate  

pip install -e ".[dev]"

pip install -r dev-requirements.txt
```

Dependencies chính bao gồm:
- Dagster ecosystem (dagster, dagstermill, dagster-aws, dagster-postgres, dagster-deltalake)
- Data processing (pandas, pyarrow, numpy, scipy, scikit-learn)
- Database (sqlalchemy, psycopg2-binary)
- Web scraping (requests, beautifulsoup4)
- Cloud storage (boto3)
- Analytics (duckdb, seaborn, matplotlib, folium)
- Development (pytest, notebook)

### 3. Khởi động MinIO storage
```bash
minio server /tmp/minio/
```

MinIO sẽ chạy tại:
- **API Endpoint**: `http://127.0.0.1:9000`
- **Username**: `minioadmin`
- **Password**: `minioadmin`

### 4. Chạy pipeline đầy đủ

```bash
# Khởi động Dagster UI
dagster dev

# Mở http://127.0.0.1:3000 và chạy job scrape_realestate
# Pipeline sẽ tự động: Scrape → Delta Lake → PostgreSQL Export → Analytics
```

### 5. Chạy analytics queries (tùy chọn)

```bash
cd src/pipelines/real-estate

# Chạy analytics queries trên dữ liệu PostgreSQL
python postgres_analytics.py
```

## Sử dụng pipeline

### Chạy pipeline đầy đủ

1. Khởi động MinIO và Dagster:
```bash
# Terminal 1: MinIO
minio server /tmp/minio/

# Terminal 2: Dagster
dagster dev
```

2. Mở Dagster UI tại http://127.0.0.1:3000
3. Chọn job `scrape_realestate` và launch

**Pipeline sẽ tự động thực hiện:**
- ✅ Scrape dữ liệu từ nhadat247.com.vn
- ✅ Lưu vào Delta Lake trên MinIO
- ✅ Export dữ liệu sang PostgreSQL
- ✅ Chạy data exploration notebook

## 🔍 Data Exploration

### PostgreSQL Analytics

Sau khi export dữ liệu sang PostgreSQL, có thể sử dụng SQL queries trực tiếp cho analytics:

```sql
-- Thống kê cơ bản
SELECT 
    COUNT(*) as total_properties,
    AVG(muc_gia::float) as avg_price,
    MIN(muc_gia::float) as min_price,
    MAX(muc_gia::float) as max_price,
    AVG(dien_tich::float) as avg_area
FROM real_estate_properties
WHERE muc_gia IS NOT NULL AND dien_tich IS NOT NULL;

-- Giá theo khu vực
SELECT 
    ia_chi,
    COUNT(*) as property_count,
    AVG(muc_gia::float) as avg_price
FROM real_estate_properties
WHERE ia_chi IS NOT NULL
GROUP BY ia_chi
ORDER BY avg_price DESC;

-- Phân tích theo loại bất động sản
SELECT 
    property_type,
    COUNT(*) as count,
    AVG(muc_gia::float) as avg_price,
    AVG(dien_tich::float) as avg_area
FROM real_estate_properties
WHERE property_type IS NOT NULL
GROUP BY property_type
ORDER BY count DESC;
```

### Python Analytics với PostgreSQL

Sử dụng SQLAlchemy hoặc pandas để kết nối và phân tích:

```python
import pandas as pd
import sqlalchemy as sa
import matplotlib.pyplot as plt
import seaborn as sns

# Kết nối PostgreSQL
engine = sa.create_engine('postgresql://user:password@host:port/database')

# Query dữ liệu
query = """
SELECT 
    muc_gia::float as price,
    dien_tich::float as area,
    ia_chi as location,
    latitude,
    longitude
FROM real_estate_properties
WHERE muc_gia IS NOT NULL 
  AND dien_tich IS NOT NULL 
  AND latitude IS NOT NULL
"""

df = pd.read_sql(query, engine)

# Visualization
plt.figure(figsize=(12, 8))
sns.scatterplot(data=df, x='area', y='price', alpha=0.6)
plt.title('Giá bất động sản theo diện tích')
plt.xlabel('Diện tích (m²)')
plt.ylabel('Giá (tỷ VNĐ)')
plt.show()

# Thống kê theo khu vực
location_stats = df.groupby('location').agg({
    'price': ['count', 'mean', 'median'],
    'area': 'mean'
}).round(2)

print(location_stats.head(10))
```

Sau khi pipeline hoàn thành, sử dụng script `postgres_analytics.py` để chạy analytics queries trên dữ liệu PostgreSQL:

```bash
cd src/pipelines/real-estate

# Chạy analytics queries trên dữ liệu PostgreSQL
python postgres_analytics.py

# Với tùy chọn giới hạn kết quả
python postgres_analytics.py --limit-results 20
```


### Jupyter Notebook (Tùy chọn)

Pipeline cũng hỗ trợ Jupyter notebook với DuckDB để query data từ Delta Lake trên MinIO:

```python
import duckdb
import pandas as pd

# Cấu hình kết nối MinIO
duckdb.sql("""
INSTALL httpfs;
LOAD httpfs;
SET s3_endpoint='127.0.0.1:9000';
SET s3_access_key_id='minioadmin';
SET s3_secret_access_key='minioadmin';
""")

# Query data từ Delta Lake
df = duckdb.sql("SELECT * FROM read_parquet(['s3://real-estate/lake/bronze/property/*.parquet'])").df()
```

### Data Schema

Dữ liệu thu thập bao gồm:
- `url`: Link bài đăng
- `tieu_e`: Tiêu đề bất động sản  
- `muc_gia`: Giá (tỷ/triệu VNĐ)
- `dien_tich`: Diện tích (m²)
- `ia_chi`: Địa chỉ chi tiết
- `latitude/longitude`: Tọa độ GPS
- `propertydetails_propertyid`: ID unique (hash từ URL)
- `ngay_ang`: Ngày thu thập dữ liệu

### Export tích hợp trong Pipeline

Export dữ liệu từ Delta Lake sang PostgreSQL đã được tích hợp trực tiếp vào pipeline Dagster. Khi chạy job `scrape_realestate`, pipeline sẽ tự động:

1. ✅ **Scrape dữ liệu** từ nhadat247.com.vn
2. ✅ **Lưu vào Delta Lake** trên MinIO S3-compatible
3. ✅ **Export sang PostgreSQL** với schema auto-detection
4. ✅ **Tạo indexes** cho performance
5. ✅ **Verify dữ liệu** sau export


### Cấu hình PostgreSQL

File `postgres_credentials.yaml` chứa thông tin kết nối:

```yaml
postgresql:
  host: your-postgres-host
  port: 5432
  database: your-database
  user: your-username
  password: your-password
```

## Acknowledgments

- [Dagster](https://dagster.io/) - Workflow orchestration
- [Delta Lake](https://delta.io/) - Data lakehouse
- [PostgreSQL](https://postgresql.org/) - Relational database
- [SQLAlchemy](https://sqlalchemy.org/) - Python SQL toolkit
- [MinIO](https://min.io/) - Object storage
- [nhadat247.com.vn](https://nhadat247.com.vn) - Data source

