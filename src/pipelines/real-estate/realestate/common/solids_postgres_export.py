#!/usr/bin/env python3
"""
Solids for PostgreSQL export operations in Dagster pipeline
"""

import logging
from typing import Dict, Any
from dagster import op, Out, get_dagster_logger

logger = logging.getLogger(__name__)

@op(description="Export data from Delta Lake to PostgreSQL", out={"result": Out(io_manager_key="fs_io_manager")})
def export_to_postgres_op(context, merged_data: Dict[str, Any], search_criterias) -> Dict[str, Any]:
    """
    Export dữ liệu từ Delta Lake (MinIO) sang PostgreSQL

    Args:
        merged_data: Dữ liệu đã được merge vào Delta Lake

    Returns:
        Dict chứa thông tin export
    """
    log = get_dagster_logger()

    try:
        # Import MinioToPostgresExporter
        import sys
        import os
        sys.path.append(os.path.dirname(os.path.dirname(__file__)))

        from postgres_analytics import MinioToPostgresExporter

        # Cấu hình PostgreSQL
        POSTGRES_CONFIG = {}

        # Tìm file credentials
        possible_paths = [
            'postgres_credentials.yaml',
            os.path.join(os.path.dirname(__file__), '..', '..', 'postgres_credentials.yaml')
        ]

        creds_file = None
        for path in possible_paths:
            if os.path.exists(path):
                creds_file = path
                break

        if creds_file:
            import yaml
            try:
                with open(creds_file, 'r', encoding='utf-8') as f:
                    creds = yaml.safe_load(f)
                POSTGRES_CONFIG['host'] = creds.get('postgresql', {}).get('host')
                POSTGRES_CONFIG['port'] = creds.get('postgresql', {}).get('port')
                POSTGRES_CONFIG['database'] = creds.get('postgresql', {}).get('database')
                POSTGRES_CONFIG['user'] = creds.get('postgresql', {}).get('user')
                POSTGRES_CONFIG['password'] = creds.get('postgresql', {}).get('password')
                log.info(f"✅ Đã tải credentials từ: {creds_file}")
            except Exception as e:
                log.warning(f"⚠️ Lỗi khi tải credentials: {e}")

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

        # Kết nối PostgreSQL
        exporter.connect_postgres()

        # Lấy danh sách file Parquet
        bucket_name = "real-estate"
        prefix = "lake/bronze/property/"
        parquet_files = exporter.get_parquet_files(bucket_name, prefix)

        if not parquet_files:
            log.error(f"❌ Không tìm thấy file Parquet nào trong s3://{bucket_name}/{prefix}")
            return {"success": False, "message": "No Parquet files found"}

        # Kết nối DuckDB MinIO để đọc data
        exporter.connect_minio_duckdb()

        # Đếm tổng số dòng
        total_expected = 0
        for file_url in parquet_files:
            # Convert s3:// to http://
            if file_url.startswith('s3://'):
                bucket_and_key = file_url[5:]
                http_url = f"{MINIO_CONFIG['endpoint']}/{bucket_and_key}"
            else:
                http_url = file_url

            count_df = exporter.duckdb_conn.sql(f"SELECT COUNT(*) as cnt FROM read_parquet('{http_url}')").df()
            total_expected += count_df['cnt'].iloc[0]

        log.info(f"📊 Tổng số dòng cần export: {total_expected}")

        # Export dữ liệu
        table_name = "real_estate_properties"

        # Lấy search criteria đầu tiên từ dynamic output
        search_criteria = {}
        if hasattr(search_criterias, '__iter__'):
            # Nếu là dynamic output, lấy item đầu tiên
            try:
                first_item = next(iter(search_criterias)) if search_criterias else {}
                search_criteria = first_item if isinstance(first_item, dict) else {}
            except:
                search_criteria = {}
        elif isinstance(search_criterias, dict):
            search_criteria = search_criterias

        log.info(f"🔍 Export với search criteria: {search_criteria}")
        exporter.export_data(parquet_files, table_name, search_criteria)

        # Tạo indexes
        exporter.create_indexes(table_name)

        # Verify
        exporter.verify_export(table_name, total_expected)

        log.info("✅ Export dữ liệu sang PostgreSQL hoàn tất thành công!")

        return {
            "success": True,
            "table_name": table_name,
            "total_records": total_expected,
            "message": f"Successfully exported {total_expected} records to PostgreSQL"
        }

    except Exception as e:
        log.error(f"❌ Lỗi khi export dữ liệu sang PostgreSQL: {e}")
        return {
            "success": False,
            "error": str(e),
            "message": "Failed to export data to PostgreSQL"
        }