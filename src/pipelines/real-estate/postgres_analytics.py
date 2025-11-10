#!/usr/bin/env python3
"""
PostgreSQL Analytics Runner - Chạy analytics queries trên dữ liệu bất động sản
"""

import os
import sys
import logging
import pandas as pd
import duckdb
import boto3
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import yaml
from typing import Optional, List, Dict, Any
from sql_query_generator import RealEstateSQLGenerator
import argparse

# Cấu hình logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class MinioToPostgresExporter:
    """Class để chạy analytics queries trên dữ liệu PostgreSQL"""

    def __init__(self, postgres_url: str, minio_config: Dict[str, Any]):
        """
        Khởi tạo exporter

        Args:
            postgres_url: PostgreSQL connection URL
            minio_config: Cấu hình MinIO (endpoint, credentials, etc.)
        """
        self.postgres_url = postgres_url
        self.minio_config = minio_config
        self.engine = None
        self.duckdb_conn = None

    def connect_postgres(self):
        """Kết nối tới PostgreSQL"""
        try:
            self.engine = create_engine(
                self.postgres_url,
                pool_size=10,
                max_overflow=20,
                pool_timeout=30,
                pool_recycle=3600
            )
            # Test connection
            with self.engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info("✅ Kết nối PostgreSQL thành công")
        except SQLAlchemyError as e:
            logger.error(f"❌ Lỗi kết nối PostgreSQL: {e}")
            raise

    def connect_minio_duckdb(self):
        """Kết nối tới MinIO qua DuckDB"""
        try:
            # Cấu hình DuckDB để kết nối MinIO S3
            self.duckdb_conn = duckdb.connect()
            self.duckdb_conn.execute(f"""
                INSTALL httpfs;
                LOAD httpfs;
                SET s3_endpoint='{self.minio_config['endpoint']}';
                SET s3_access_key_id='{self.minio_config['access_key']}';
                SET s3_secret_access_key='{self.minio_config['secret_key']}';
                SET s3_use_ssl=false;
                SET s3_url_style='path';
            """)
            logger.info("✅ Kết nối MinIO qua DuckDB thành công")
        except Exception as e:
            logger.error(f"❌ Lỗi kết nối MinIO: {e}")
            raise

    def create_table_from_parquet(self, file_url: str, table_name: str, batch_size: int = 1000):
        """
        Tạo bảng PostgreSQL từ file Parquet và insert dữ liệu

        Args:
            file_url: URL của file Parquet trong MinIO
            table_name: Tên bảng PostgreSQL
            batch_size: Kích thước batch cho insert
        """
        try:
            # Convert s3:// URL to http:// URL for DuckDB
            if file_url.startswith('s3://'):
                # s3://bucket/key -> http://endpoint/bucket/key
                bucket_and_key = file_url[5:]  # Remove 's3://'
                http_url = f"{self.minio_config['endpoint']}/{bucket_and_key}"
            else:
                http_url = file_url

            logger.info(f"📄 Đọc file từ: {http_url}")

            # Đọc sample để xác định schema
            df_sample = self.duckdb_conn.sql(f"SELECT * FROM read_parquet('{http_url}') LIMIT 1").df()

            if df_sample.empty:
                logger.warning(f"⚠️ File {http_url} trống, bỏ qua")
                return

            # Mapping kiểu dữ liệu từ pandas sang PostgreSQL
            type_mapping = {
                'object': 'TEXT',
                'int64': 'BIGINT',
                'float64': 'DOUBLE PRECISION',
                'bool': 'BOOLEAN',
                'datetime64[ns]': 'TIMESTAMP'
            }

            # Sanitize column names for PostgreSQL
            import unicodedata
            import re
            
            def sanitize_column_name(name):
                # Remove accents and special characters
                name = unicodedata.normalize('NFD', str(name))
                name = name.encode('ascii', 'ignore').decode('ascii')
                # Replace spaces and special chars with underscore
                name = re.sub(r'[^a-zA-Z0-9]', '_', name)
                # Remove multiple underscores
                name = re.sub(r'_+', '_', name)
                # Remove leading/trailing underscores
                name = name.strip('_')
                # Ensure not empty
                if not name:
                    name = 'column'
                return name.lower()

            # Tạo câu lệnh CREATE TABLE
            columns = []
            column_mapping = {}  # Map original name to sanitized name
            for col, dtype in df_sample.dtypes.items():
                sanitized_col = sanitize_column_name(col)
                pg_type = type_mapping.get(str(dtype), 'TEXT')
                columns.append(f'"{sanitized_col}" {pg_type}')
                column_mapping[col] = sanitized_col

            create_table_sql = f"""
            DROP TABLE IF EXISTS {table_name};
            CREATE TABLE {table_name} (
                {', '.join(columns)}
            );
            """

            # Thực thi CREATE TABLE
            with self.engine.connect() as conn:
                conn.execute(text(create_table_sql))
                conn.commit()

            logger.info(f"✅ Đã tạo bảng {table_name}")

            # Insert dữ liệu theo batch
            offset = 0
            while True:
                batch_df = self.duckdb_conn.sql(f"SELECT * FROM read_parquet('{http_url}') LIMIT {batch_size} OFFSET {offset}").df()
                if batch_df.empty:
                    break

                # Rename columns to sanitized names
                batch_df_renamed = batch_df.rename(columns=column_mapping)

                # Insert batch
                batch_df_renamed.to_sql(table_name, self.engine, if_exists='append', index=False)
                offset += batch_size
                logger.info(f"📥 Đã insert {len(batch_df)} dòng (offset: {offset})")

        except Exception as e:
            logger.error(f"❌ Lỗi khi tạo bảng từ {file_url}: {e}")
            raise

    def get_parquet_files(self, bucket_name: str, prefix: str) -> List[str]:
        """Lấy danh sách file Parquet từ MinIO sử dụng boto3"""
        try:
            # Sử dụng boto3 để kết nối MinIO
            s3_client = boto3.client(
                's3',
                endpoint_url=self.minio_config['endpoint'],
                aws_access_key_id=self.minio_config['access_key'],
                aws_secret_access_key=self.minio_config['secret_key'],
                region_name=self.minio_config.get('region', 'us-east-1')
            )

            # List objects với prefix
            response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
            
            files = []
            if 'Contents' in response:
                for obj in response['Contents']:
                    key = obj['Key']
                    if key.endswith('.parquet'):
                        files.append(f"s3://{bucket_name}/{key}")
            
            logger.info(f"📁 Tìm thấy {len(files)} file Parquet")
            for file in files[:5]:  # Log first 5 files
                logger.info(f"  - {file}")
            return files
        except Exception as e:
            logger.error(f"❌ Lỗi khi lấy danh sách file với boto3: {e}")
            return []

    def export_data(self, parquet_files: List[str], table_name: str):
        """Export tất cả dữ liệu từ Parquet files sang PostgreSQL"""
        logger.info(f"🚀 Bắt đầu export {len(parquet_files)} file Parquet sang {table_name}")

        for i, file_url in enumerate(parquet_files, 1):
            logger.info(f"📄 Đang xử lý file {i}/{len(parquet_files)}: {file_url}")
            self.create_table_from_parquet(file_url, table_name)

        logger.info("✅ Hoàn thành export dữ liệu")

    def create_indexes(self, table_name: str):
        """Tạo indexes cho bảng để tối ưu performance"""
        # Chỉ tạo index cho các cột chắc chắn tồn tại
        indexes = [
            f"CREATE INDEX IF NOT EXISTS idx_{table_name}_ia_chi ON {table_name}(ia_chi);",
            f"CREATE INDEX IF NOT EXISTS idx_{table_name}_url ON {table_name}(url);",
            f"CREATE INDEX IF NOT EXISTS idx_{table_name}_latitude ON {table_name}(latitude);",
            f"CREATE INDEX IF NOT EXISTS idx_{table_name}_longitude ON {table_name}(longitude);"
        ]

        try:
            with self.engine.connect() as conn:
                for index_sql in indexes:
                    conn.execute(text(index_sql))
                    conn.commit()
            logger.info("✅ Đã tạo indexes")
        except Exception as e:
            logger.error(f"❌ Lỗi khi tạo indexes: {e}")

    def verify_export(self, table_name: str, expected_count: int):
        """Verify số lượng dữ liệu đã export"""
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(f"SELECT COUNT(*) FROM {table_name}"))
                actual_count = result.fetchone()[0]

            logger.info(f"📊 Số dòng trong PostgreSQL: {actual_count}")
            logger.info(f"📊 Số dòng expected: {expected_count}")

            if actual_count == expected_count:
                logger.info("✅ Số lượng dữ liệu khớp!")
            else:
                logger.warning(f"⚠️ Số lượng không khớp: {actual_count} vs {expected_count}")
        except Exception as e:
            logger.error(f"❌ Lỗi khi verify: {e}")

    def generate_and_run_queries(self, table_name: str, run_queries: bool = True, limit_results: int = 10) -> Dict[str, Any]:
        """Generate và chạy các SQL queries analytics"""
        sql_gen = RealEstateSQLGenerator()
        results = {}

        queries = {
            'basic_stats': sql_gen.generate_basic_stats_query(table_name),
            'price_by_city': sql_gen.generate_price_by_city_query(table_name),
            'location_distribution': sql_gen.generate_property_type_distribution_query(table_name),
            'price_ranges': sql_gen.generate_price_ranges_query(table_name)
        }

        if run_queries:
            try:
                for query_name, query_sql in queries.items():
                    logger.info(f"🔍 Chạy query: {query_name}")
                    with self.engine.connect() as conn:
                        result = conn.execute(text(query_sql))
                        df = pd.DataFrame(result.fetchall(), columns=result.keys())
                        results[query_name] = df.head(limit_results)
            except Exception as e:
                logger.error(f"❌ Lỗi khi chạy queries: {e}")

        return results

    def print_query_results(self, results: Dict[str, Any]):
        """In kết quả queries"""
        for query_name, df in results.items():
            print(f"\n=== {query_name.upper()} ===")
            print(df.to_string(index=False))

def main():
    parser = argparse.ArgumentParser(description='Chạy analytics queries trên dữ liệu bất động sản trong PostgreSQL')
    parser.add_argument('--limit-results', type=int, default=10, help='Giới hạn số dòng kết quả queries')

    args = parser.parse_args()

    # Cấu hình PostgreSQL mặc định
    POSTGRES_CONFIG = {}

    # Tìm file credentials
    possible_paths = [
        'postgres_credentials.yaml',
        os.path.join(os.path.dirname(__file__), 'postgres_credentials.yaml')
    ]

    creds_file = None
    for path in possible_paths:
        if os.path.exists(path):
            creds_file = path
            break

    if creds_file:
        try:
            with open(creds_file, 'r', encoding='utf-8') as f:
                creds = yaml.safe_load(f)
            POSTGRES_CONFIG['host'] = creds.get('postgresql', {}).get('host')
            POSTGRES_CONFIG['port'] = creds.get('postgresql', {}).get('port')
            POSTGRES_CONFIG['database'] = creds.get('postgresql', {}).get('database')
            POSTGRES_CONFIG['user'] = creds.get('postgresql', {}).get('user')
            POSTGRES_CONFIG['password'] = creds.get('postgresql', {}).get('password')
            logger.info(f"✅ Đã tải credentials từ: {creds_file}")
        except Exception as e:
            logger.warning(f"⚠️ Lỗi khi tải credentials: {e}")

    # Fallback defaults
    if not POSTGRES_CONFIG.get('host'):
        POSTGRES_CONFIG['host'] = 'localhost'
    if not POSTGRES_CONFIG.get('port'):
        POSTGRES_CONFIG['port'] = 5432
    if not POSTGRES_CONFIG.get('database'):
        POSTGRES_CONFIG['database'] = 'real_estate_db'
    if not POSTGRES_CONFIG.get('user'):
        POSTGRES_CONFIG['user'] = 'postgres'
    if not POSTGRES_CONFIG.get('password'):
        POSTGRES_CONFIG['password'] = 'postgres123'

    # Cấu hình MinIO
    MINIO_CONFIG = {
        'endpoint': 'http://127.0.0.1:9000',
        'access_key': 'minioadmin',
        'secret_key': 'minioadmin',
        'region': 'us-east-1',
        'use_ssl': False
    }

    # Tạo PostgreSQL URL
    postgres_url = f"postgresql://{POSTGRES_CONFIG['user']}:{POSTGRES_CONFIG['password']}@{POSTGRES_CONFIG['host']}:{POSTGRES_CONFIG['port']}/{POSTGRES_CONFIG['database']}"

    # Khởi tạo exporter
    exporter = MinioToPostgresExporter(postgres_url, MINIO_CONFIG)

    try:
        # Kết nối PostgreSQL
        exporter.connect_postgres()

        # Chỉ chạy queries trên dữ liệu đã có
        table_name = "real_estate_properties"
        logger.info("🔍 Generating và chạy các câu lệnh SQL truy vấn...")
        query_results = exporter.generate_and_run_queries(
            table_name=table_name,
            run_queries=True,
            limit_results=args.limit_results
        )
        exporter.print_query_results(query_results)

        logger.info("🎉 Hoàn thành generate queries!")

    except Exception as e:
        logger.error(f"❌ Lỗi trong quá trình chạy queries: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()