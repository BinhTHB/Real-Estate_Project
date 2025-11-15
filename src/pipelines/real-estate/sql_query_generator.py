#!/usr/bin/env python3
"""
SQL Query Generator cho Real Estate Analytics
"""

import logging
from typing import Dict, List

logger = logging.getLogger(__name__)

class RealEstateSQLGenerator:
    """Class để tạo các SQL queries cho analytics bất động sản"""

    def __init__(self):
        self.queries = {}

    def generate_basic_stats_query(self, table_name: str) -> str:
        """Tạo query thống kê cơ bản"""
        return f"""
        SELECT
            COUNT(*) as total_properties,
            to_char(AVG(CAST(regexp_replace(muc_gia, '[^0-9.]', '', 'g') AS FLOAT)), 'FM999,999,999,999') as avg_price,
            to_char(MIN(CAST(regexp_replace(muc_gia, '[^0-9.]', '', 'g') AS FLOAT)), 'FM999,999,999,999') as min_price,
            to_char(MAX(CAST(regexp_replace(muc_gia, '[^0-9.]', '', 'g') AS FLOAT)), 'FM999,999,999,999') as max_price,
            to_char(AVG(CAST(regexp_replace(dien_tich, '[^0-9.]', '', 'g') AS FLOAT)), 'FM999,999,999.99') as avg_area,
            COUNT(DISTINCT ia_chi) as cities_count
        FROM {table_name}
        WHERE muc_gia IS NOT NULL
          AND dien_tich IS NOT NULL
          AND regexp_replace(muc_gia, '[^0-9.]', '', 'g') != ''
          AND regexp_replace(dien_tich, '[^0-9.]', '', 'g') != '';
        """

    def generate_price_by_city_query(self, table_name: str) -> str:
        """Tạo query giá theo thành phố"""
        return f"""
        SELECT
            ia_chi as city,
            COUNT(*) as property_count,
            to_char(AVG(CAST(regexp_replace(muc_gia, '[^0-9.]', '', 'g') AS FLOAT)), 'FM999,999,999,999') as avg_price,
            to_char(MIN(CAST(regexp_replace(muc_gia, '[^0-9.]', '', 'g') AS FLOAT)), 'FM999,999,999,999') as min_price,
            to_char(MAX(CAST(regexp_replace(muc_gia, '[^0-9.]', '', 'g') AS FLOAT)), 'FM999,999,999,999') as max_price,
            to_char(AVG(CAST(regexp_replace(muc_gia, '[^0-9.]', '', 'g') AS FLOAT) /
                NULLIF(CAST(regexp_replace(dien_tich, '[^0-9.]', '', 'g') AS FLOAT), 0)), 'FM999,999,999,999') as avg_price_per_sqm
        FROM {table_name}
        WHERE CAST(regexp_replace(muc_gia, '[^0-9.]', '', 'g') AS FLOAT) > 0
          AND CAST(regexp_replace(dien_tich, '[^0-9.]', '', 'g') AS FLOAT) > 0
          AND ia_chi IS NOT NULL
          AND regexp_replace(muc_gia, '[^0-9.]', '', 'g') != ''
          AND regexp_replace(dien_tich, '[^0-9.]', '', 'g') != ''
        GROUP BY ia_chi
        ORDER BY avg_price DESC;
        """

    def generate_property_type_distribution_query(self, table_name: str) -> str:
        """Tạo query phân bố theo khu vực (thay vì loại bất động sản)"""
        return f"""
        SELECT
            ia_chi as location,
            COUNT(*) as count,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage,
            to_char(AVG(CAST(regexp_replace(muc_gia, '[^0-9.]', '', 'g') AS FLOAT)), 'FM999,999,999,999') as avg_price,
            to_char(AVG(CAST(regexp_replace(dien_tich, '[^0-9.]', '', 'g') AS FLOAT)), 'FM999,999,999.99') as avg_area
        FROM {table_name}
        WHERE ia_chi IS NOT NULL
          AND regexp_replace(muc_gia, '[^0-9.]', '', 'g') != ''
          AND regexp_replace(dien_tich, '[^0-9.]', '', 'g') != ''
        GROUP BY ia_chi
        ORDER BY count DESC
        LIMIT 20;
        """

    def generate_recent_trends_query(self, table_name: str, days: int = 30) -> str:
        """Tạo query xu hướng gần đây"""
        return f"""
        SELECT
            DATE(ngay_ang) as date,
            COUNT(*) as daily_count,
            to_char(AVG(CAST(regexp_replace(muc_gia, '[^0-9.]', '', 'g') AS FLOAT)), 'FM999,999,999,999') as avg_price,
            to_char(AVG(CAST(regexp_replace(dien_tich, '[^0-9.]', '', 'g') AS FLOAT)), 'FM999,999,999.99') as avg_area
        FROM {table_name}
        WHERE ngay_ang IS NOT NULL
          AND regexp_replace(muc_gia, '[^0-9.]', '', 'g') != ''
          AND regexp_replace(dien_tich, '[^0-9.]', '', 'g') != ''
        GROUP BY DATE(ngay_ang)
        ORDER BY date DESC
        LIMIT 30;
        """

    def generate_price_ranges_query(self, table_name: str) -> str:
        """Tạo query phân tích khoảng giá"""
        return f"""
        SELECT
            CASE
                WHEN CAST(regexp_replace(muc_gia, '[^0-9.]', '', 'g') AS FLOAT) < 1000000000 THEN '< 1 tỷ'
                WHEN CAST(regexp_replace(muc_gia, '[^0-9.]', '', 'g') AS FLOAT) < 3000000000 THEN '1-3 tỷ'
                WHEN CAST(regexp_replace(muc_gia, '[^0-9.]', '', 'g') AS FLOAT) < 5000000000 THEN '3-5 tỷ'
                WHEN CAST(regexp_replace(muc_gia, '[^0-9.]', '', 'g') AS FLOAT) < 10000000000 THEN '5-10 tỷ'
                ELSE '> 10 tỷ'
            END as price_range,
            COUNT(*) as count,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) as percentage,
            to_char(AVG(CAST(regexp_replace(dien_tich, '[^0-9.]', '', 'g') AS FLOAT)), 'FM999,999,999.99') as avg_area
        FROM {table_name}
        WHERE CAST(regexp_replace(muc_gia, '[^0-9.]', '', 'g') AS FLOAT) > 0
          AND regexp_replace(muc_gia, '[^0-9.]', '', 'g') != ''
        GROUP BY
            CASE
                WHEN CAST(regexp_replace(muc_gia, '[^0-9.]', '', 'g') AS FLOAT) < 1000000000 THEN '< 1 tỷ'
                WHEN CAST(regexp_replace(muc_gia, '[^0-9.]', '', 'g') AS FLOAT) < 3000000000 THEN '1-3 tỷ'
                WHEN CAST(regexp_replace(muc_gia, '[^0-9.]', '', 'g') AS FLOAT) < 5000000000 THEN '3-5 tỷ'
                WHEN CAST(regexp_replace(muc_gia, '[^0-9.]', '', 'g') AS FLOAT) < 10000000000 THEN '5-10 tỷ'
                ELSE '> 10 tỷ'
            END
        ORDER BY MIN(CAST(regexp_replace(muc_gia, '[^0-9.]', '', 'g') AS FLOAT));
        """

    def generate_top_expensive_areas_query(self, table_name: str, limit: int = 10) -> str:
        """Tạo query top khu vực đắt đỏ"""
        return f"""
        SELECT
            COALESCE(district, city) as area,
            COUNT(*) as property_count,
            to_char(AVG(price), 'FM999,999,999,999') as avg_price,
            to_char(MAX(price), 'FM999,999,999,999') as max_price,
            to_char(AVG(price/area), 'FM999,999,999,999') as price_per_sqm
        FROM {table_name}
        WHERE price > 0 AND area > 0
        GROUP BY COALESCE(district, city)
        HAVING COUNT(*) >= 5
        ORDER BY avg_price DESC
        LIMIT {limit};
        """

    def generate_all_queries(self, table_name: str) -> Dict[str, str]:
        """Tạo tất cả các queries"""
        return {
            'basic_stats': self.generate_basic_stats_query(table_name),
            'price_by_city': self.generate_price_by_city_query(table_name),
            'property_types': self.generate_property_type_distribution_query(table_name),
            'recent_trends': self.generate_recent_trends_query(table_name),
            'price_ranges': self.generate_price_ranges_query(table_name),
            'top_expensive_areas': self.generate_top_expensive_areas_query(table_name)
        }

    def get_query_descriptions(self) -> Dict[str, str]:
        """Mô tả các queries"""
        return {
            'basic_stats': 'Thống kê cơ bản: tổng số, giá trung bình, diện tích',
            'price_by_city': 'Giá bất động sản theo thành phố',
            'property_types': 'Phân bố loại bất động sản',
            'recent_trends': 'Xu hướng giá gần đây (30 ngày)',
            'price_ranges': 'Phân tích theo khoảng giá',
            'top_expensive_areas': 'Top khu vực đắt đỏ nhất'
        }

    def print_available_queries(self):
        """In ra danh sách queries có sẵn"""
        descriptions = self.get_query_descriptions()
        print("📊 Available Real Estate Analytics Queries:")
        print("=" * 50)
        for query_name, description in descriptions.items():
            print(f"• {query_name}: {description}")
        print("=" * 50)


def main():
    """Demo function"""
    generator = RealEstateSQLGenerator()
    generator.print_available_queries()

    # Example usage
    table_name = "real_estate_properties"
    queries = generator.generate_all_queries(table_name)

    print(f"\n🔍 Generated {len(queries)} queries for table '{table_name}'")
    print("\nExample - Basic Stats Query:")
    print(queries['basic_stats'])


if __name__ == "__main__":
    main()