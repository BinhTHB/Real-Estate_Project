# Real Estate Project

Một pipeline data engineering thực tế để thu thập, xử lý và phân tích dữ liệu bất động sản Việt Nam từ nhadat247.com.vn.

## Mục lục

- [Kiến trúc dự án](#-kiến-trúc-dự-án)
- [Tính năng chính](#-tính-năng-chính)
- [Công nghệ sử dụng](#️-công-nghệ-sử-dụng)
- [Cài đặt và chạy](#-cài-đặt-và-chạy)
- [Sử dụng pipeline](#-sử-dụng-pipeline)
- [Data Exploration](#-data-exploration)
- [Cấu trúc thư mục](#-cấu-trúc-thư-mục)

## Kiến trúc dự án

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Web Scraping  │ -> │   Data Process  │ -> │   Delta Lake    │ -> │   Data Explore  │
│  (Requests + BS)│    │   (Pandas)      │    │   (MinIO S3)    │    │   (Jupyter)     │
└─────────────────┘    └─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │                       │
         └───────────────────────┼───────────────────────┼───────────────────────┘
                                 │                       │
                    ┌─────────────────┐         ┌─────────────────┐
                    │    Dagster     │         │    Analytics    │
                    │ Orchestration  │         │   (DuckDB)      │
                    └─────────────────┘         └─────────────────┘
```

### Luồng xử lý dữ liệu:
1. **Scraping Layer**: Thu thập dữ liệu từ nhadat247.com.vn sử dụng requests + BeautifulSoup, tránh DNS issues
2. **Processing Layer**: Xử lý dữ liệu với Pandas, chuẩn hóa format
3. **Storage Layer**: Lưu trữ ACID với Delta Lake trên MinIO S3-compatible
4. **Exploration Layer**: Phân tích dữ liệu với Jupyter notebooks và DuckDB

**Key Data Flow**: Search Criteria → URL Generation → Threaded Requests Scraping → DataFrame Processing → Delta Lake Merge → Jupyter Exploration

## Tính năng chính

- ✅ **Web Scraping ổn định**: Sử dụng requests + BeautifulSoup, tránh DNS issues của Chrome/Selenium
- ✅ **ACID Transactions**: Delta Lake đảm bảo tính toàn vẹn dữ liệu
- ✅ **Schema Evolution**: Tự động adapt khi schema thay đổi
- ✅ **Cloud Storage**: MinIO S3-compatible cho storage agnostic
- ✅ **Data Exploration**: Jupyter notebooks với DuckDB analytics
- ✅ **Monitoring**: Dagster UI cho pipeline monitoring

## 🛠️ Công nghệ sử dụng

### Core Dependencies
- **Dagster 1.6.8**: Workflow orchestration và pipeline management
- **Dagster-DeltaLake-Pandas**: Delta Lake integration với Pandas
- **Delta Lake**: ACID transactions và time travel cho data lake
- **MinIO**: S3-compatible object storage
- **PyArrow**: Apache Arrow cho data processing
- **Pandas**: Data manipulation và analysis
- **DuckDB**: In-process analytical database (sử dụng trong notebooks)
- **Requests**: HTTP client cho web scraping
- **BeautifulSoup4**: HTML parsing
- **Boto3**: AWS S3 API client (MinIO compatible)

### Development & Deployment
- **Dagstermill**: Jupyter notebook integration với Dagster

## Cài đặt và chạy

### Prerequisites
- Python 3.8+
- Git
- Windows OS

### 1. Clone repository
```bash
git clone https://github.com/BinhTHB/Real-Estate_Project.git
cd Real-Estate_Project
```

### 2. Cài đặt dependencies
```bash
cd src/pipelines/real-estate

# Kích hoạt virtual environment (nếu có)
# .\venv\Scripts\activate  

pip install -e ".[dev]"
```

Dependencies chính bao gồm:
- Dagster ecosystem (dagster, dagstermill, dagster-aws, dagster-postgres, dagster-deltalake)
- Data processing (pandas, pyarrow, numpy, scipy, scikit-learn)
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

### 4. Startup dagster
```bash
dagster dev
```

## Sử dụng pipeline

### Chạy pipeline scraping

1. Mở Dagster UI tại http://127.0.0.1:3000
2. Chọn job `scrape_realestate`
3. Launch với configuration mặc định hoặc tùy chỉnh:

```yaml
# scrape_realestate.yaml
solids:
  collect_search_criterias:
    inputs:
      search_criterias:
        - city: "hanoi"
          propertyType: "can-ho-chung-cu"
          rentOrBuy: "buy"
          radius: 0
```

### Monitoring pipeline

Dagster UI cung cấp:
- ✅ **Pipeline runs**: Lịch sử executions
- ✅ **Logs**: Chi tiết từng step
- ✅ **Data lineage**: Flow của data
- ✅ **Asset catalog**: Datasets được tạo

## 🔍 Data Exploration

### Jupyter Notebook

Pipeline tự động chạy notebook `main_notebook.ipynb` sau khi scrape data. Notebook sử dụng DuckDB để query data từ Delta Lake trên MinIO:

```python
# Trong notebook có thể:
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

# Analytics với DuckDB
result = duckdb.sql("""
    SELECT
        "Mức giá",
        "Diện tích", 
        latitude,
        longitude,
        COUNT(*) as count
    FROM df
    GROUP BY "Mức giá", "Diện tích", latitude, longitude
""").df()
```

### Data Schema

Dữ liệu thu thập bao gồm:
- `url`: Link bài đăng
- `Tiêu đề`: Tiêu đề bất động sản  
- `Mức giá`: Giá (tỷ/triệu VNĐ)
- `Diện tích`: Diện tích (m²)
- `Địa chỉ`: Địa chỉ chi tiết
- `latitude/longitude`: Tọa độ GPS
- `propertyDetails_propertyId`: ID unique (hash từ URL)
- `Ngày đăng`: Ngày thu thập dữ liệu

## Cấu trúc thư mục

```
Real-Estate_Project/
├── src/
│   └── pipelines/
│       └── real-estate/
│           ├── realestate/           # Core pipeline code
│           │   ├── pipelines.py      # Main job definitions và orchestration
│           │   ├── resources.py      # Dagster resources (database/S3 configs)
│           │   ├── common/           # Shared utilities
│           │   │   ├── requests_scraping.py    # Web scraping logic (requests + BS4)
│           │   │   ├── solids_spark_delta.py   # Delta Lake operations (merge/upsert)
│           │   │   ├── types_realestate.py     # Custom data types
│           │   │   ├── helper_functions.py     # Utility functions
│           │   │   ├── solids_jupyter.py       # Notebook integration
│           │   │   └── resources.py            # Resource definitions (boto3, etc.)
│           │   ├── config_environments/        # Environment configs (local/prod)
│           │   ├── config_pipelines/          # Pipeline execution parameters
│           │   └── notebooks/                 # Data exploration notebooks
│           ├── setup.py                       # Package setup với dependencies
│           ├── pyproject.toml                 # Project metadata
│           ├── dev-requirements.txt           # Development dependencies
│           └── tox.ini                        # Testing configuration
├── lake/bronze/                  # Delta Lake storage (runtime)
├── PRODUCTION_FEATURES_GUIDE.md   # Production deployment guide
├── .github/copilot-instructions.md # AI assistant instructions
└── README.md                      # This file
```

## Acknowledgments

- [Dagster](https://dagster.io/) - Workflow orchestration
- [Delta Lake](https://delta.io/) - Data lakehouse
- [MinIO](https://min.io/) - Object storage
- [nhadat247.com.vn](https://nhadat247.com.vn) - Data source

