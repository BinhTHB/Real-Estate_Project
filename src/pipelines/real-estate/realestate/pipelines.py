# realestate/pipelines.py
from dagster import (
    job,
    op,
    graph,
    Out,
    In,
    DynamicOut,
    DynamicOutput,
    config_from_files,
    file_relative_path,
)
from typing import List, Dict

import pandas as pd
import logging

from realestate.common import resource_def
from realestate.common.types_realestate import PropertyDataFrame, SearchCoordinate
from realestate.common.solids_spark_delta import merge_property_delta, flatten_json
from realestate.common.solids_jupyter import data_exploration

# Import our stable requests scrapper (no Selenium DNS issues)
from realestate.common.requests_scraping import requests_scraping, _empty_df

# Import PostgreSQL export functionality
from realestate.common.solids_postgres_export import export_to_postgres_op

logger = logging.getLogger(__name__)


@op(description="Generate URLs from search criteria", out=Out(io_manager_key="fs_io_manager"))
def generate_urls_from_criteria(context, search_criteria: Dict) -> List[str]:
    """
    Đây chỉ trả về list URL (các trang kết quả) dựa trên search_criteria.
    search_criteria có thể là dict hoặc tuple/list có 1 phần tử dict.
    """
    # normalize potential dagster wrapping (tuple/list length 1)
    if isinstance(search_criteria, (tuple, list)) and len(search_criteria) == 1:
        search_criteria = search_criteria[0]

    context.log.info(f"🔹 Generating URLs for: {search_criteria}")
    city = search_criteria.get("city", "")
    propType = search_criteria.get("propertyType", "")
    rentOrBuy = search_criteria.get("rentOrBuy", "buy")

    city_slug = city.replace(" ", "-").lower() if city else ""
    # Comprehensive mapping for all Vietnamese provinces and cities
    city_map = {
        # Major cities
        "hanoi": "ha-noi-xc1",
        "ha-noi": "ha-noi-xc1",
        "hà-nội": "ha-noi-xc1",
        "hà nội": "ha-noi-xc1",
        "ho-chi-minh": "ho-chi-minh-xc79",
        "hochiminh": "ho-chi-minh-xc79",
        "ho-chi-minh-city": "ho-chi-minh-xc79",
        "sai-gon": "ho-chi-minh-xc79",
        "saigon": "ho-chi-minh-xc79",
        "tp-hcm": "ho-chi-minh-xc79",
        "tphcm": "ho-chi-minh-xc79",
        "da-nang": "da-nang-xc48",
        "danang": "da-nang-xc48",
        "đà-nẵng": "da-nang-xc48",
        "đà nẵng": "da-nang-xc48",
        "hai-phong": "hai-phong-xc2",
        "haiphong": "hai-phong-xc2",
        "hải-phòng": "hai-phong-xc2",
        "hải phòng": "hai-phong-xc2",
        "can-tho": "can-tho-xc55",
        "cantho": "can-tho-xc55",
        "cần-thơ": "can-tho-xc55",
        "cần thơ": "can-tho-xc55",

        # Northern provinces
        "bac-giang": "bac-giang-xc3",
        "bac-giang": "bac-giang-xc3",
        "bac-kan": "bac-kan-xc4",
        "bắc-kạn": "bac-kan-xc4",
        "bắc kạn": "bac-kan-xc4",
        "bac-ninh": "bac-ninh-xc5",
        "bắc-ninh": "bac-ninh-xc5",
        "bắc ninh": "bac-ninh-xc5",
        "cao-bang": "cao-bang-xc6",
        "cao-bang": "cao-bang-xc6",
        "dien-bien": "dien-bien-xc7",
        "điện-biên": "dien-bien-xc7",
        "điện biên": "dien-bien-xc7",
        "gia-lam": "gia-lam-xc8",
        "gia-lâm": "gia-lam-xc8",
        "gia lâm": "gia-lam-xc8",
        "ha-giang": "ha-giang-xc9",
        "hà-giang": "ha-giang-xc9",
        "hà giang": "ha-giang-xc9",
        "ha-nam": "ha-nam-xc10",
        "hà-nam": "ha-nam-xc10",
        "hà nam": "ha-nam-xc10",
        "ha-tinh": "ha-tinh-xc11",
        "hà-tĩnh": "ha-tinh-xc11",
        "hà tĩnh": "ha-tinh-xc11",
        "hung-yen": "hung-yen-xc12",
        "hưng-yên": "hung-yen-xc12",
        "hưng yên": "hung-yen-xc12",
        "lai-chau": "lai-chau-xc13",
        "lai-châu": "lai-chau-xc13",
        "lai châu": "lai-chau-xc13",
        "lang-son": "lang-son-xc14",
        "lạng-sơn": "lang-son-xc14",
        "lạng sơn": "lang-son-xc14",
        "lao-cai": "lao-cai-xc15",
        "lào-cai": "lao-cai-xc15",
        "lào cai": "lao-cai-xc15",
        "nam-dinh": "nam-dinh-xc16",
        "nam-định": "nam-dinh-xc16",
        "nam định": "nam-dinh-xc16",
        "nghe-an": "nghe-an-xc17",
        "nghệ-an": "nghe-an-xc17",
        "nghệ an": "nghe-an-xc17",
        "ninh-binh": "ninh-binh-xc18",
        "ninh-bình": "ninh-binh-xc18",
        "ninh bình": "ninh-binh-xc18",
        "phu-tho": "phu-tho-xc19",
        "phú-thọ": "phu-tho-xc19",
        "phú thọ": "phu-tho-xc19",
        "quang-ninh": "quang-ninh-xc20",
        "quảng-ninh": "quang-ninh-xc20",
        "quảng ninh": "quang-ninh-xc20",
        "son-la": "son-la-xc21",
        "sơn-la": "son-la-xc21",
        "sơn la": "son-la-xc21",
        "thai-binh": "thai-binh-xc22",
        "thái-bình": "thai-binh-xc22",
        "thái bình": "thai-binh-xc22",
        "thai-nguyen": "thai-nguyen-xc23",
        "thái-nguyên": "thai-nguyen-xc23",
        "thái nguyên": "thai-nguyen-xc23",
        "thanh-hoa": "thanh-hoa-xc24",
        "thanh-hóa": "thanh-hoa-xc24",
        "thanh hóa": "thanh-hoa-xc24",
        "tuyen-quang": "tuyen-quang-xc25",
        "tuyên-quang": "tuyen-quang-xc25",
        "tuyên quang": "tuyen-quang-xc25",
        "vinh-phuc": "vinh-phuc-xc26",
        "vĩnh-phúc": "vinh-phuc-xc26",
        "vĩnh phúc": "vinh-phuc-xc26",
        "yen-bai": "yen-bai-xc27",
        "yên-bái": "yen-bai-xc27",
        "yên bái": "yen-bai-xc27",

        # Central provinces
        "binh-dinh": "binh-dinh-xc28",
        "bình-định": "binh-dinh-xc28",
        "bình định": "binh-dinh-xc28",
        "binh-thuan": "binh-thuan-xc29",
        "bình-thuận": "binh-thuan-xc29",
        "bình thuận": "binh-thuan-xc29",
        "da-nang": "da-nang-xc48",
        "đà-nẵng": "da-nang-xc48",
        "đà nẵng": "da-nang-xc48",
        "dak-lak": "dak-lak-xc30",
        "đắk-lắk": "dak-lak-xc30",
        "đắk lắk": "dak-lak-xc30",
        "dak-nong": "dak-nong-xc31",
        "đắk-nông": "dak-nong-xc31",
        "đắk nông": "dak-nong-xc31",
        "gia-lai": "gia-lai-xc32",
        "gia-lai": "gia-lai-xc32",
        "ha-tinh": "ha-tinh-xc11",
        "hà-tĩnh": "ha-tinh-xc11",
        "hà tĩnh": "ha-tinh-xc11",
        "khanh-hoa": "khanh-hoa-xc33",
        "khánh-hòa": "khanh-hoa-xc33",
        "khánh hòa": "khanh-hoa-xc33",
        "kon-tum": "kon-tum-xc34",
        "kon-tum": "kon-tum-xc34",
        "lam-dong": "lam-dong-xc35",
        "lâm-đồng": "lam-dong-xc35",
        "lâm đồng": "lam-dong-xc35",
        "nghe-an": "nghe-an-xc17",
        "nghệ-an": "nghe-an-xc17",
        "nghệ an": "nghe-an-xc17",
        "phu-yen": "phu-yen-xc36",
        "phú-yên": "phu-yen-xc36",
        "phú yên": "phu-yen-xc36",
        "quang-binh": "quang-binh-xc37",
        "quảng-bình": "quang-binh-xc37",
        "quảng bình": "quang-binh-xc37",
        "quang-nam": "quang-nam-xc38",
        "quảng-nam": "quang-nam-xc38",
        "quảng nam": "quang-nam-xc38",
        "quang-ngai": "quang-ngai-xc39",
        "quảng-ngãi": "quang-ngai-xc39",
        "quảng ngãi": "quang-ngai-xc39",
        "quang-tri": "quang-tri-xc40",
        "quảng-trị": "quang-tri-xc40",
        "quảng trị": "quang-tri-xc40",
        "thua-thien-hue": "thua-thien-hue-xc41",
        "thừa-thiên-huế": "thua-thien-hue-xc41",
        "thừa thiên huế": "thua-thien-hue-xc41",

        # Southern provinces
        "an-giang": "an-giang-xc42",
        "an-giang": "an-giang-xc42",
        "ba-ria-vung-tau": "ba-ria-vung-tau-xc43",
        "bà-rịa-vũng-tàu": "ba-ria-vung-tau-xc43",
        "bà rịa vũng tàu": "ba-ria-vung-tau-xc43",
        "bac-lieu": "bac-lieu-xc44",
        "bạc-liêu": "bac-lieu-xc44",
        "bạc liêu": "bac-lieu-xc44",
        "ben-tre": "ben-tre-xc45",
        "bến-tre": "ben-tre-xc45",
        "bến tre": "ben-tre-xc45",
        "binh-duong": "binh-duong-xc46",
        "bình-dương": "binh-duong-xc46",
        "bình dương": "binh-duong-xc46",
        "binh-phuoc": "binh-phuoc-xc47",
        "bình-phước": "binh-phuoc-xc47",
        "bình phước": "binh-phuoc-xc47",
        "ca-mau": "ca-mau-xc49",
        "cà-mau": "ca-mau-xc49",
        "cà mau": "ca-mau-xc49",
        "can-tho": "can-tho-xc55",
        "cần-thơ": "can-tho-xc55",
        "cần thơ": "can-tho-xc55",
        "dong-nai": "dong-nai-xc50",
        "đồng-nai": "dong-nai-xc50",
        "đồng nai": "dong-nai-xc50",
        "dong-thap": "dong-thap-xc51",
        "đồng-tháp": "dong-thap-xc51",
        "đồng tháp": "dong-thap-xc51",
        "hai-duong": "hai-duong-xc52",
        "hải-dương": "hai-duong-xc52",
        "hải dương": "hai-duong-xc52",
        "hau-giang": "hau-giang-xc53",
        "hậu-giang": "hau-giang-xc53",
        "hậu giang": "hau-giang-xc53",
        "hoa-binh": "hoa-binh-xc54",
        "hòa-bình": "hoa-binh-xc54",
        "hòa bình": "hoa-binh-xc54",
        "hung-yen": "hung-yen-xc12",
        "hưng-yên": "hung-yen-xc12",
        "hưng yên": "hung-yen-xc12",
        "kien-giang": "kien-giang-xc56",
        "kiên-giang": "kien-giang-xc56",
        "kiên giang": "kien-giang-xc56",
        "long-an": "long-an-xc57",
        "long-an": "long-an-xc57",
        "soc-trang": "soc-trang-xc58",
        "sóc-trăng": "soc-trang-xc58",
        "sóc trăng": "soc-trang-xc58",
        "tay-ninh": "tay-ninh-xc59",
        "tây-ninh": "tay-ninh-xc59",
        "tây ninh": "tay-ninh-xc59",
        "tien-giang": "tien-giang-xc60",
        "tiền-giang": "tien-giang-xc60",
        "tiền giang": "tien-giang-xc60",
        "tra-vinh": "tra-vinh-xc61",
        "trà-vinh": "tra-vinh-xc61",
        "trà vinh": "tra-vinh-xc61",
        "vinh-long": "vinh-long-xc62",
        "vĩnh-long": "vinh-long-xc62",
        "vĩnh long": "vinh-long-xc62"
    }
    city_code = city_map.get(city_slug, "")

    if city_code:
        base = f"https://nhadat247.com.vn/mua-ban-nha-dat-{city_code}-ci38.html"
    else:
        base = "https://nhadat247.com.vn/mua-ban-nha-dat-ci38.html"

    # hiện tại chỉ trả 1 trang, dùng max_pages nếu cần mở rộng
    return [base]


@op(description="Run requests-based scraping and return DataFrame", out=Out(io_manager_key="fs_io_manager"))
def requests_scraping_op(context, urls: List[str]) -> pd.DataFrame:
    """
    Stable requests-based scraping without Selenium DNS issues.
    """
    # Get config for production scaling - handle None case
    limit_each_page = 5  # Default value
    if context.op_config and isinstance(context.op_config, dict):
        limit_each_page = context.op_config.get("limit_each_page", 5)

    context.log.info(f"🚀 Starting requests scraping with {len(urls)} URLs, limit_each_page={limit_each_page}...")

    df = requests_scraping(
        urls=urls,
        limit_each_page=limit_each_page,
    )

    # ensure DataFrame
    if isinstance(df, list):
        df = pd.DataFrame(df)

    if df is None or (isinstance(df, pd.DataFrame) and df.empty):
        context.log.warning("⚠️ No data scraped. Returning empty DataFrame with schema.")
        df = _empty_df()

    context.log.info(f"✅ Scraped {len(df)} properties.")
    return df


@op(description="Collects Search Criteria and create dynamic outputs", out=DynamicOut(io_manager_key="fs_io_manager"))
def collect_search_criterias(context, search_criterias: List[SearchCoordinate]):
    for search in search_criterias:
        key = (
            "_".join(
                [
                    search["city"],
                    search["rentOrBuy"],
                    search["propertyType"],
                    str(search.get("radius", 0)),
                ]
            )
            .replace("-", "_")
            .lower()
        )

        yield DynamicOutput(search, mapping_key=key)


@op(description="Collect results list into single list for merging", out=Out(io_manager_key="fs_io_manager"))
def collect_properties(properties):
    # flatten nested lists if necessary
    flat = []
    for p in properties:
        if isinstance(p, list):
            flat.extend(p)
        elif isinstance(p, pd.DataFrame):
            # convert df rows to dicts
            flat.extend(p.to_dict(orient="records"))
        else:
            flat.append(p)
    return flat


@graph(description="Scrape properties using requests (no Selenium DNS issues)")
def requests_scrape_properties(search_criteria):
    urls = generate_urls_from_criteria(search_criteria)
    return requests_scraping_op(urls)


@graph(description="Merge scraped data into Delta table")
def merge_staging_to_delta_table_composite(properties):
    # properties may be list/dict/df -> normalize in merge op
    return merge_property_delta(input_dataframe=properties)


@job(
    resource_defs=resource_def["local"],
    config=config_from_files(
        [
            file_relative_path(__file__, "config_environments/local_base.yaml"),
            file_relative_path(__file__, "config_pipelines/scrape_realestate.yaml"),
        ]
    ),
)
def scrape_realestate():
    search_criterias = collect_search_criterias().map(requests_scrape_properties)
    merge = merge_staging_to_delta_table_composite.alias("merge_staging_to_delta_table")
    merged_data = merge(collect_properties(search_criterias.collect()))

    # Export to PostgreSQL after merging
    export_result = export_to_postgres_op(merged_data)

    # Run data exploration
    data_exploration(merged_data)
