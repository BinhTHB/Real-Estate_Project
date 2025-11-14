# Tiểu luận: Xây dựng Pipeline cho Phân tích Dữ liệu Bất động sản Việt Nam

## 1. Trình bày chủ đề tiểu luận

### 1.1 Giới thiệu chủ đề
Trong bối cảnh thị trường bất động sản Việt Nam phát triển nhanh chóng với lượng dữ liệu khổng lồ từ các nguồn trực tuyến, việc thu thập, xử lý và phân tích dữ liệu một cách hiệu quả trở thành yếu tố then chốt cho các nhà đầu tư, nhà phát triển và nhà quản lý. Dự án này tập trung vào việc xây dựng một pipeline data engineering toàn diện để thu thập dữ liệu bất động sản từ website nhadat247.com.vn, xử lý và lưu trữ dữ liệu theo chuẩn data lakehouse, và cung cấp các công cụ phân tích dữ liệu mạnh mẽ.

### 1.2 Mục tiêu nghiên cứu
- Xây dựng hệ thống pipeline tự động thu thập dữ liệu bất động sản từ web
- Triển khai kiến trúc data lakehouse với Delta Lake để đảm bảo tính toàn vẹn dữ liệu
- Tích hợp PostgreSQL cho việc phân tích dữ liệu quan hệ
- Phát triển các công cụ analytics và visualization cho data exploration
- Đảm bảo tính ổn định, khả năng mở rộng và dễ bảo trì của hệ thống

### 1.3 Ý nghĩa thực tiễn
Dự án giải quyết các vấn đề thực tế trong việc xử lý big data bất động sản, cung cấp nền tảng cho việc ra quyết định dựa trên dữ liệu, hỗ trợ phân tích thị trường và dự báo xu hướng bất động sản Việt Nam.

## 2. Nội dung, kịch bản và triển khai các nội dung

### 2.1 Nội dung chính của dự án
Dự án bao gồm các module cốt lõi sau:
- **Web Scraping Module**: Thu thập dữ liệu từ nhadat247.com.vn
- **Data Processing Module**: Chuẩn hóa và xử lý dữ liệu với Pandas
- **Storage Module**: Lưu trữ dữ liệu với Delta Lake trên MinIO S3
- **Database Module**: Export dữ liệu sang PostgreSQL
- **Analytics Module**: Các công cụ phân tích và visualization

### 2.2 Kịch bản sử dụng
#### Kịch bản thu thập dữ liệu
1. Định nghĩa tiêu chí tìm kiếm bất động sản (khu vực, loại hình, giá cả)
2. Tạo URL tìm kiếm từ tiêu chí
3. Thu thập dữ liệu song song từ nhiều trang
4. Xử lý và chuẩn hóa dữ liệu thu thập được

#### Kịch bản phân tích dữ liệu
1. Xuất dữ liệu từ Delta Lake sang PostgreSQL
2. Chạy các truy vấn SQL để thống kê cơ bản
3. Phân tích xu hướng giá theo khu vực và loại hình
4. Visualization dữ liệu với biểu đồ và bản đồ

#### Kịch bản monitoring và maintenance
1. Giám sát pipeline thông qua Dagster UI
2. Kiểm tra chất lượng dữ liệu
3. Cập nhật schema khi cần thiết
4. Backup và recovery dữ liệu

### 2.3 Triển khai các nội dung
#### Triển khai Web Scraping
- Sử dụng requests + BeautifulSoup để tránh vấn đề DNS
- Triển khai multi-threading cho hiệu suất cao
- Xử lý các trường hợp edge case và rate limiting

#### Triển khai Data Processing
- Chuẩn hóa format dữ liệu (giá, diện tích, tọa độ)
- Validation dữ liệu và xử lý missing values
- Tạo unique identifier cho mỗi bất động sản

#### Triển khai Storage Layer
- Cấu hình Delta Lake trên MinIO S3-compatible
- Implement schema evolution tự động
- Đảm bảo ACID transactions

#### Triển khai Analytics
- Tích hợp PostgreSQL export vào pipeline
- Phát triển SQL queries cho các phân tích phổ biến
- Tạo Jupyter notebooks cho interactive analysis

## 3. Xây dựng sản phẩm/giải pháp

### 3.1 Kiến trúc tổng thể
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

### 3.2 Công nghệ sử dụng

#### Core Dependencies
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

#### Development & Deployment
- **Dagstermill**: Jupyter notebook integration với Dagster
- **PostgreSQL Export Op**: Tích hợp export vào pipeline Dagster

### 3.3 Cài đặt và triển khai

#### 1. Clone repository
```bash
git clone https://github.com/BinhTHB/Real-Estate_Project.git
cd Real-Estate_Project
```

#### 2. Cài đặt dependencies
```bash
cd src/pipelines/real-estate

python -m venv venv

.\venv\Scripts\activate  

pip install -r requirements.txt
```

#### 3. Khởi động MinIO storage
```bash
minio server /tmp/minio/
```

MinIO sẽ chạy tại:
- **API Endpoint**: `http://127.0.0.1:9000`
- **Username**: `minioadmin`
- **Password**: `minioadmin`

#### 4. Chạy pipeline đầy đủ
```bash
# Khởi động Dagster UI
dagster dev

# Mở http://127.0.0.1:3000 và chạy job scrape_realestate
# Pipeline sẽ tự động: Scrape → Delta Lake → PostgreSQL Export → Analytics
```

### 3.4 Tính năng chính
- ✅ **Web Scraping ổn định**: Sử dụng requests + BeautifulSoup, tránh DNS issues của Chrome/Selenium
- ✅ **ACID Transactions**: Delta Lake đảm bảo tính toàn vẹn dữ liệu
- ✅ **Schema Evolution**: Tự động adapt khi schema thay đổi
- ✅ **Cloud Storage**: MinIO S3-compatible cho storage agnostic
- ✅ **PostgreSQL Integration**: Pipeline tự động export từ Delta Lake sang PostgreSQL
- ✅ **Analytics Queries**: Script chuyên dụng để chạy SQL analytics trên PostgreSQL
- ✅ **Data Exploration**: Jupyter notebooks với DuckDB và PostgreSQL queries
- ✅ **Monitoring**: Dagster UI cho pipeline monitoring

### 3.5 Data Schema
Dữ liệu thu thập bao gồm:
- `url`: Link bài đăng
- `tieu_e`: Tiêu đề bất động sản  
- `muc_gia`: Giá (tỷ/triệu VNĐ)
- `dien_tich`: Diện tích (m²)
- `ia_chi`: Địa chỉ chi tiết
- `latitude/longitude`: Tọa độ GPS
- `propertydetails_propertyid`: ID unique (hash từ URL)
- `ngay_ang`: Ngày thu thập dữ liệu

### 3.6 Cách vận hành và triển khai hệ thống

#### Quy trình vận hành pipeline
Hệ thống được thiết kế theo quy trình tuần hoàn với các giai đoạn chính:

1. **Giai đoạn Thu thập dữ liệu (Data Ingestion)**:
   - Pipeline bắt đầu bằng việc đọc các tiêu chí tìm kiếm từ file cấu hình
   - Tạo ra hàng nghìn URL tìm kiếm dựa trên các combination của khu vực, loại hình và khoảng giá
   - Mỗi URL được xử lý song song để tối ưu hiệu suất
   - Dữ liệu thô từ website được thu thập và lưu trữ tạm thời

2. **Giai đoạn Xử lý dữ liệu (Data Processing)**:
   - Dữ liệu thô được làm sạch và chuẩn hóa format
   - Các trường số như giá và diện tích được chuyển đổi từ text sang numeric
   - Địa chỉ được geocoding để lấy tọa độ GPS
   - Mỗi bất động sản được gán một ID duy nhất để tránh trùng lặp

3. **Giai đoạn Lưu trữ (Data Storage)**:
   - Dữ liệu đã xử lý được ghi vào Delta Lake trên MinIO
   - Sử dụng format Parquet để tối ưu storage và query performance
   - Mỗi lần chạy pipeline tạo ra một version mới của dữ liệu
   - ACID transactions đảm bảo data consistency

4. **Giai đoạn Xuất dữ liệu (Data Export)**:
   - Dữ liệu từ Delta Lake được đồng bộ sang PostgreSQL
   - Sử dụng UPSERT logic để cập nhật records hiện có và thêm records mới
   - Tạo các indexes để tối ưu performance cho queries
   - Verify data integrity sau khi export

#### Chiến lược triển khai production
Để triển khai hệ thống trên môi trường production:

1. **Infrastructure Setup**:
   - Chạy MinIO trên dedicated server hoặc cloud storage (AWS S3, GCS)
   - PostgreSQL database trên managed service (AWS RDS, Google Cloud SQL)
   - Dagster daemon chạy trên container orchestration platform

2. **Configuration Management**:
   - Sử dụng environment variables cho sensitive credentials
   - File YAML cho pipeline configurations
   - Version control cho tất cả configuration files

3. **Monitoring và Alerting**:
   - Dagster UI cung cấp real-time monitoring của pipeline runs
   - Logs được centralized để dễ dàng troubleshooting
   - Alerts được gửi khi pipeline fails hoặc performance degrades

4. **Scheduling và Automation**:
   - Pipeline được schedule chạy định kỳ (daily/weekly)
   - Automated retries cho failed operations
   - Backup strategies cho data và metadata

#### Quy trình maintenance và updates
Để duy trì và cập nhật hệ thống:

1. **Regular Monitoring**:
   - Theo dõi performance metrics (runtime, throughput, error rates)
   - Kiểm tra data quality và completeness
   - Monitor storage usage và costs

2. **Update Procedures**:
   - Test changes trên staging environment trước khi deploy production
   - Gradual rollout với feature flags
   - Rollback plans cho critical updates

3. **Scalability Planning**:
   - Monitor resource usage patterns
   - Plan capacity upgrades dựa trên data growth
   - Optimize queries và storage khi cần thiết

#### Disaster Recovery
Chiến lược phục hồi sau sự cố:

1. **Data Backup**:
   - Delta Lake snapshots được lưu trữ regularly
   - PostgreSQL backups với point-in-time recovery
   - Cross-region replication cho high availability

2. **Failover Procedures**:
   - Automatic failover cho database và storage
   - Manual procedures cho complex failures
   - Communication plans cho stakeholders

3. **Business Continuity**:
   - Alternative data sources khi website chính không available
   - Cached data serving khi pipeline down
   - Service level agreements (SLAs) cho uptime

#### Performance Optimization Strategies
Để đảm bảo hiệu suất tối ưu:

1. **Data Processing**:
   - Parallel processing cho large datasets
   - Memory-efficient algorithms
   - Incremental updates thay vì full refreshes

2. **Storage Optimization**:
   - Data partitioning theo date/location
   - Compression để giảm storage costs
   - Query optimization với appropriate indexes

3. **Network Efficiency**:
   - Connection pooling cho database connections
   - Batch operations thay vì individual requests
   - CDN integration cho static assets

## 4. Kết luận

### 4.1 Tổng kết dự án
Dự án đã thành công xây dựng một pipeline data engineering toàn diện cho việc thu thập và phân tích dữ liệu bất động sản Việt Nam. Hệ thống tích hợp các công nghệ hiện đại như Dagster, Delta Lake, và PostgreSQL, đảm bảo tính ổn định, khả năng mở rộng và dễ bảo trì.

### 4.2 Kết quả đạt được
- **Pipeline tự động**: Thu thập dữ liệu từ web một cách ổn định và hiệu quả
- **Data Lakehouse**: Lưu trữ dữ liệu với ACID transactions và schema evolution
- **Analytics sẵn sàng**: Cung cấp các công cụ phân tích SQL và Python
- **Monitoring toàn diện**: Dagster UI cho việc giám sát và debugging

### 4.3 Hạn chế và hướng phát triển
#### Hạn chế hiện tại:
- Phụ thuộc vào cấu trúc HTML của website nguồn
- Cần cải thiện xử lý rate limiting và anti-bot measures
- Yêu cầu tài nguyên cho việc chạy MinIO locally

#### Hướng phát triển tương lai:
- Mở rộng nguồn dữ liệu từ nhiều website bất động sản khác
- Triển khai trên cloud (AWS, GCP, Azure) với managed services
- Phát triển machine learning models cho dự đoán giá bất động sản
- Tạo dashboard web cho real-time analytics
- Implement data quality monitoring và alerting

### 4.4 Bài học kinh nghiệm
Dự án cho thấy tầm quan trọng của việc lựa chọn công nghệ phù hợp và thiết kế kiến trúc linh hoạt. Việc sử dụng data lakehouse pattern giúp giải quyết nhiều vấn đề truyền thống của data warehouse, trong khi orchestration tools như Dagster giúp quản lý pipeline phức tạp một cách hiệu quả.

## Acknowledgments

- [Dagster](https://dagster.io/) - Workflow orchestration
- [Delta Lake](https://delta.io/) - Data lakehouse
- [PostgreSQL](https://postgresql.org/) - Relational database
- [SQLAlchemy](https://sqlalchemy.org/) - Python SQL toolkit
- [MinIO](https://min.io/) - Object storage
- [nhadat247.com.vn](https://nhadat247.com.vn) - Data source

