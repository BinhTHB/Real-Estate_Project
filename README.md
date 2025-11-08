# 🏠 Real Estate Project

[![Dagster](https://img.shields.io/badge/Dagster-1.6.8-blue)](https://dagster.io/)
[![Delta Lake](https://img.shields.io/badge/Delta_Lake-3.0.0-green)](https://delta.io/)
[![MinIO](https://img.shields.io/badge/MinIO-S3-orange)](https://min.io/)
[![Python](https://img.shields.io/badge/Python-3.8+-yellow)](https://python.org/)

Một pipeline data engineering thực tế để thu thập, xử lý và phân tích dữ liệu bất động sản Việt Nam từ nhadat247.com.vn.

## 📋 Mục lục

- [🏗️ Kiến trúc dự án](#-kiến-trúc-dự-án)
- [✨ Tính năng chính](#-tính-năng-chính)
- [🛠️ Công nghệ sử dụng](#️-công-nghệ-sử-dụng)
- [🚀 Cài đặt và chạy](#-cài-đặt-và-chạy)
- [📊 Sử dụng pipeline](#-sử-dụng-pipeline)
- [🔍 Data Exploration](#-data-exploration)
- [📁 Cấu trúc thư mục](#-cấu-trúc-thư-mục)
- [🤝 Đóng góp](#-đóng-góp)

## 🏗️ Kiến trúc dự án

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Web Scraping  │ -> │   Data Process  │ -> │   Delta Lake    │ -> │   Data Explore  │
│  (Requests + BS)│    │   (Pandas)      │    │   (MinIO S3)    │    │   (Jupyter)     │
└─────────────────┘    └─────────────────┘    └─────────────────┘    └─────────────────┘
         │                       │                       │                       │
         └───────────────────────┼───────────────────────┼───────────────────────┘
                                 │                       │
                    ┌─────────────────┐         ┌─────────────────┐
                    │    Dagster     │         │    DuckDB       │
                    │ Orchestration  │         │   Analytics     │
                    └─────────────────┘         └─────────────────┘
```

### Luồng xử lý dữ liệu:
1. **Scraping Layer**: Thu thập dữ liệu từ nhadat247.com.vn sử dụng requests + BeautifulSoup
2. **Processing Layer**: Xử lý dữ liệu với Pandas, chuẩn hóa format
3. **Storage Layer**: Lưu trữ ACID với Delta Lake trên MinIO S3-compatible
4. **Exploration Layer**: Phân tích dữ liệu với Jupyter notebooks và DuckDB

## ✨ Tính năng chính

- ✅ **Web Scraping ổn định**: Sử dụng requests thay vì Selenium, tránh DNS issues
- ✅ **ACID Transactions**: Delta Lake đảm bảo tính toàn vẹn dữ liệu
- ✅ **Schema Evolution**: Tự động adapt khi schema thay đổi
- ✅ **Cloud Storage**: MinIO S3-compatible cho storage agnostic
- ✅ **Data Exploration**: Jupyter notebooks với DuckDB analytics
- ✅ **Monitoring**: Dagster UI cho pipeline monitoring

## 🛠️ Công nghệ sử dụng

### Core Dependencies
- **Dagster 1.6.8**: Workflow orchestration và pipeline management
- **Delta Lake**: ACID transactions và time travel cho data lake
- **MinIO**: S3-compatible object storage
- **PyArrow**: Apache Arrow cho data processing
- **Pandas**: Data manipulation và analysis
- **DuckDB**: In-process analytical database

### Scraping & Processing
- **Requests**: HTTP client cho web scraping
- **BeautifulSoup4**: HTML parsing
- **Boto3**: AWS S3 API client (MinIO compatible)

### Development & Deployment
- **Dagstermill**: Jupyter notebook integration với Dagster

## 🚀 Cài đặt và chạy

### Prerequisites
- Python 3.8+
- Git
- Windows OS

### 1. Clone repository
```bash
git clone https://github.com/BinhTHB/Real-Estate_Project_Data_Engineering.git
cd Real-Estate_Project_Data_Engineering
```

### 2. Cài đặt dependencies
```bash
cd src/pipelines/real-estate
pip install -e ".[dev]"
```

### 3. Khởi động MinIO storage
```bash
# Windows
.\MinIO_run.bat
```

MinIO sẽ chạy tại:
- **API Endpoint**: `http://127.0.0.1:9000`
- **Web Console**: `http://127.0.0.1:9001`
- **Username**: `minioadmin`
- **Password**: `minioadmin`

### 4. Quick deployment (Alternative)
```bash
# Run everything with one command
.\deploy_production.bat
```

Script này sẽ tự động:
- Khởi động MinIO storage
- Chờ MinIO sẵn sàng
- Khởi động Dagster development server
- Hiển thị tất cả access points
```bash
dagster dev --port 3000
```

Truy cập Dagster UI tại: http://localhost:3000

## 📊 Sử dụng pipeline

### Chạy pipeline scraping

1. Mở Dagster UI tại http://localhost:3000
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

Pipeline tự động chạy notebook `TEST.ipynb` sau khi scrape data:

```python
# Trong notebook có thể:
import duckdb
import pandas as pd

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

## 📁 Cấu trúc thư mục

```
Real-Estate_Project_Data_Engineering/
├── src/
│   └── pipelines/
│       └── real-estate/
│           ├── realestate/           # Core pipeline code
│           │   ├── common/           # Shared utilities
│           │   │   ├── requests_scraping.py    # Web scraping logic
│           │   │   ├── solids_spark_delta.py   # Delta Lake operations
│           │   │   ├── solids_jupyter.py       # Notebook integration
│           │   │   └── types_realestate.py     # Custom types
│           │   ├── config_environments/        # Environment configs
│           │   ├── config_pipelines/          # Pipeline configs
│           │   ├── notebooks/                 # Data exploration
│           │   ├── pipelines.py               # Main pipeline definition
│           │   └── resources.py               # Dagster resources
│           ├── setup.py                       # Package setup
│           └── pyproject.toml                 # Project metadata
├── MinIO_run.bat                  # MinIO startup script
├── deploy_production.bat          # Production deployment script
├── PRODUCTION_FEATURES_GUIDE.md   # Production features documentation
├── .github/copilot-instructions.md # AI assistant instructions
└── README.md                      # This file
```

## 🤝 Đóng góp

### Development setup

```bash
# Install development dependencies
pip install -e ".[dev]"

# Run tests
pytest

# Format code
black .
isort .
```

### Code quality

- **Black**: Code formatting
- **isort**: Import sorting
- **pytest**: Unit testing
- **Dagster**: Pipeline testing

### Adding new features

1. Tạo feature branch từ `main`
2. Implement changes
3. Add tests
4. Update documentation
5. Create pull request

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- [Dagster](https://dagster.io/) - Workflow orchestration
- [Delta Lake](https://delta.io/) - Data lakehouse
- [MinIO](https://min.io/) - Object storage
- [nhadat247.com.vn](https://nhadat247.com.vn) - Data source

---

**Built with ❤️ for the Vietnamese real estate data engineering community**