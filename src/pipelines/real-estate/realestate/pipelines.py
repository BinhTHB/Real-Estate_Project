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
def generate_urls_from_criteria(context, search_criteria) -> List[str]:
    """
    Đây chỉ trả về list URL (các trang kết quả) dựa trên search_criteria.
    search_criteria có thể là dict hoặc SearchCoordinate type.
    """
    # normalize potential dagster wrapping (tuple/list length 1)
    if isinstance(search_criteria, (tuple, list)) and len(search_criteria) == 1:
        search_criteria = search_criteria[0]

    # Convert SearchCoordinate to dict if needed
    if hasattr(search_criteria, '_asdict'):  # NamedTuple-like
        search_dict = search_criteria._asdict()
    elif hasattr(search_criteria, '__dict__'):  # Object with attributes
        search_dict = search_criteria.__dict__
    elif isinstance(search_criteria, dict):
        search_dict = search_criteria
    else:
        # Try to access as dict-like
        search_dict = dict(search_criteria) if hasattr(search_criteria, 'keys') else {}

    context.log.info(f"🔹 Generating URLs for: {search_dict}")
    city = search_dict.get("city", "")
    propType = search_dict.get("propertyType", "")
    rentOrBuy = search_dict.get("rentOrBuy", "buy")

    city_slug = city.replace(" ", "-").lower() if city else ""
    
    # Property type to category code mapping
    # Based on nhadat247.com.vn URL patterns
    property_type_map = {
        "can-ho-chung-cu": "ci38",  # Apartments
        "nha-rieng": "ci40",       # Houses
        "nha-mat-pho": "ci41",     # Shophouses
        "dat-nen": "ci42",         # Land
        "van-phong": "ci43",       # Offices
        "phong-tro": "ci44",       # Rooms for rent
        "real-estate": "ci38",     # General real estate
        "house": "ci40",           # Houses
        "flat": "ci38",            # Apartments
        "plot": "ci42",            # Land
        "parking-space": "ci45",   # Parking
        "multi-family-residential": "ci46", # Multi-family
        "office-commerce-industry": "ci43", # Offices
        "agriculture": "ci47",     # Agriculture
        "other-objects": "ci48",   # Other
    }
    
    # Rent/Buy action mapping
    action_map = {
        "buy": "mua-ban",
        "rent": "cho-thue"
    }
    
    # Get category code, default to ci38
    category_code = property_type_map.get(propType, "ci38")
    action = action_map.get(rentOrBuy, "mua-ban")
    
    # Comprehensive mapping for all Vietnamese provinces and cities
    city_map = {
        # Major cities
        "hanoi": "ha-noi-xc1",
        "ha-noi": "ha-noi-xc1",
        "hà-nội": "ha-noi-xc1",
        "hà nội": "ha-noi-xc1",
        "ho-chi-minh": "tp-hcm-xc79",
        "hochiminh": "tp-hcm-xc79",
        "ho-chi-minh-city": "tp-hcm-xc79",
        "sai-gon": "tp-hcm-xc79",
        "saigon": "tp-hcm-xc79",
        "tp-hcm": "tp-hcm-xc79",
        "tphcm": "tp-hcm-xc79",
        "da-nang": "da-nang-xc48",
        "danang": "da-nang-xc48",
        "đà-nẵng": "da-nang-xc48",
        "đà nẵng": "da-nang-xc48",
        "hai-phong": "hai-phong-xc31",
        "haiphong": "hai-phong-xc31",
        "hải-phòng": "hai-phong-xc31",
        "hải phòng": "hai-phong-xc31",
        "can-tho": "can-tho-xc92",
        "cantho": "can-tho-xc92",
        "cần-thơ": "can-tho-xc92",
        "cần thơ": "can-tho-xc92",

        # Northern provinces
        "bac-giang": "bac-giang-xc24",
        "bac-giang": "bac-giang-xc24",
        "bac-kan": "bac-kan-xc6",
        "bắc-kạn": "bac-kan-xc6",
        "bắc kạn": "bac-kan-xc6",
        "bac-ninh": "bac-ninh-xc27",
        "bắc-ninh": "bac-ninh-xc27",
        "bắc ninh": "bac-ninh-xc27",
        "cao-bang": "cao-bang-xc4",
        "cao-bang": "cao-bang-xc4",
        "dien-bien": "dien-bien-xc11",
        "điện-biên": "dien-bien-xc11",
        "điện biên": "dien-bien-xc11",
        "gia-lam": "gia-lam-xc8",
        "gia-lâm": "gia-lam-xc8",
        "gia lâm": "gia-lam-xc8",
        "ha-giang": "ha-giang-xc2",
        "hà-giang": "ha-giang-xc2",
        "hà giang": "ha-giang-xc2",
        "ha-nam": "ha-nam-xc35",
        "hà-nam": "ha-nam-xc35",
        "hà nam": "ha-nam-xc35",
        "ha-tinh": "ha-tinh-xc42",
        "hà-tĩnh": "ha-tinh-xc42",
        "hà tĩnh": "ha-tinh-xc42",
        "hung-yen": "hung-yen-xc33",
        "hưng-yên": "hung-yen-xc33",
        "hưng yên": "hung-yen-xc33",
        "lai-chau": "lai-chau-xc12",
        "lai-châu": "lai-chau-xc12",
        "lai châu": "lai-chau-xc12",
        "lang-son": "lang-son-xc20",
        "lạng-sơn": "lang-son-xc20",
        "lạng sơn": "lang-son-xc20",
        "lao-cai": "lao-cai-xc10",
        "lào-cai": "lao-cai-xc10",
        "lào cai": "lao-cai-xc10",
        "nam-dinh": "nam-dinh-xc36",
        "nam-định": "nam-dinh-xc36",
        "nam định": "nam-dinh-xc36",
        "nghe-an": "nghe-an-xc40",
        "nghệ-an": "nghe-an-xc40",
        "nghệ an": "nghe-an-xc40",
        "ninh-binh": "ninh-binh-xc37",
        "ninh-bình": "ninh-binh-xc37",
        "ninh bình": "ninh-binh-xc37",
        "phu-tho": "phu-tho-xc25",
        "phú-thọ": "phu-tho-xc25",
        "phú thọ": "phu-tho-xc25",
        "quang-ninh": "quang-ninh-xc22",
        "quảng-ninh": "quang-ninh-xc22",
        "quảng ninh": "quang-ninh-xc22",
        "son-la": "son-la-xc14",
        "sơn-la": "son-la-xc14",
        "sơn la": "son-la-xc14",
        "thai-binh": "thai-binh-xc34",
        "thái-bình": "thai-binh-xc34",
        "thái bình": "thai-binh-xc34",
        "thai-nguyen": "thai-nguyen-xc19",
        "thái-nguyên": "thai-nguyen-xc19",
        "thái nguyên": "thai-nguyen-xc19",
        "thanh-hoa": "thanh-hoa-xc38",
        "thanh-hóa": "thanh-hoa-xc38",
        "thanh hóa": "thanh-hoa-xc38",
        "tuyen-quang": "tuyen-quang-xc8",
        "tuyên-quang": "tuyen-quang-xc8",
        "tuyên quang": "tuyen-quang-xc8",
        "vinh-phuc": "vinh-phuc-xc26",
        "vĩnh-phúc": "vinh-phuc-xc26",
        "vĩnh phúc": "vinh-phuc-xc26",
        "yen-bai": "yen-bai-xc15",
        "yên-bái": "yen-bai-xc15",
        "yên bái": "yen-bai-xc15",

        # Central provinces
        "binh-dinh": "binh-dinh-xc52",
        "bình-định": "binh-dinh-xc52",
        "bình định": "binh-dinh-xc52",
        "binh-thuan": "binh-thuan-xc60",
        "bình-thuận": "binh-thuan-xc60",
        "bình thuận": "binh-thuan-xc60",
        "da-nang": "da-nang-xc48",
        "đà-nẵng": "da-nang-xc48",
        "đà nẵng": "da-nang-xc48",
        "dak-lak": "dak-lak-xc66",
        "đắk-lắk": "dak-lak-xc66",
        "đắk lắk": "dak-lak-xc66",
        "dak-nong": "dak-nong-xc67",
        "đắk-nông": "dak-nong-xc67",
        "đắk nông": "dak-nong-xc67",
        "gia-lai": "gia-lai-xc64",
        "gia-lai": "gia-lai-xc64",
        "ha-tinh": "ha-tinh-xc42",
        "hà-tĩnh": "ha-tinh-xc42",
        "hà tĩnh": "ha-tinh-xc42",
        "khanh-hoa": "khanh-hoa-xc56",
        "khánh-hòa": "khanh-hoa-xc56",
        "khánh hòa": "khanh-hoa-xc56",
        "kon-tum": "kon-tum-xc62",
        "kon-tum": "kon-tum-xc62",
        "lam-dong": "lam-dong-xc68",
        "lâm-đồng": "lam-dong-xc68",
        "lâm đồng": "lam-dong-xc68",
        "nghe-an": "nghe-an-xc40",
        "nghệ-an": "nghe-an-xc40",
        "nghệ an": "nghe-an-xc40",
        "phu-yen": "phu-yen-xc54",
        "phú-yên": "phu-yen-xc54",
        "phú yên": "phu-yen-xc54",
        "quang-binh": "quang-binh-xc44",
        "quảng-bình": "quang-binh-xc44",
        "quảng bình": "quang-binh-xc44",
        "quang-nam": "quang-nam-xc49",
        "quảng-nam": "quang-nam-xc49",
        "quảng nam": "quang-nam-xc49",
        "quang-ngai": "quang-ngai-xc51",
        "quảng-ngãi": "quang-ngai-xc51",
        "quảng ngãi": "quang-ngai-xc51",
        "quang-tri": "quang-tri-xc45",
        "quảng-trị": "quang-tri-xc45",
        "quảng trị": "quang-tri-xc45",
        "thua-thien-hue": "thua-thien-hue-xc46",
        "thừa-thiên-huế": "thua-thien-hue-xc46",
        "thừa thiên huế": "thua-thien-hue-xc46",

        # Southern provinces
        "an-giang": "an-giang-xc89",
        "an-giang": "an-giang-xc89",
        "ba-ria-vung-tau": "ba-ria-vung-tau-xc77",
        "bà-rịa-vũng-tàu": "ba-ria-vung-tau-xc77",
        "bà rịa vũng tàu": "ba-ria-vung-tau-xc77",
        "bac-lieu": "bac-lieu-xc95",
        "bạc-liêu": "bac-lieu-xc95",
        "bạc liêu": "bac-lieu-xc95",
        "ben-tre": "ben-tre-xc83",
        "bến-tre": "ben-tre-xc83",
        "bến tre": "ben-tre-xc83",
        "binh-duong": "binh-duong-xc74",
        "bình-dương": "binh-duong-xc74",
        "bình dương": "binh-duong-xc74",
        "binh-phuoc": "binh-phuoc-xc70",
        "bình-phước": "binh-phuoc-xc70",
        "bình phước": "binh-phuoc-xc70",
        "ca-mau": "ca-mau-xc96",
        "cà-mau": "ca-mau-xc96",
        "cà mau": "ca-mau-xc96",
        "can-tho": "can-tho-xc92",
        "cần-thơ": "can-tho-xc92",
        "cần thơ": "can-tho-xc92",
        "dong-nai": "dong-nai-xc75",
        "đồng-nai": "dong-nai-xc75",
        "đồng nai": "dong-nai-xc75",
        "dong-thap": "dong-thap-xc87",
        "đồng-tháp": "dong-thap-xc87",
        "đồng tháp": "dong-thap-xc87",
        "hai-duong": "hai-duong-xc30",
        "hải-dương": "hai-duong-xc30",
        "hải dương": "hai-duong-xc30",
        "hau-giang": "hau-giang-xc93",
        "hậu-giang": "hau-giang-xc93",
        "hậu giang": "hau-giang-xc93",
        "hoa-binh": "hoa-binh-xc17",
        "hòa-bình": "hoa-binh-xc17",
        "hòa bình": "hoa-binh-xc17",
        "hung-yen": "hung-yen-xc33",
        "hưng-yên": "hung-yen-xc33",
        "hưng yên": "hung-yen-xc33",
        "kien-giang": "kien-giang-xc91",
        "kiên-giang": "kien-giang-xc91",
        "kiên giang": "kien-giang-xc91",
        "long-an": "long-an-xc80",
        "long-an": "long-an-xc80",
        "soc-trang": "soc-trang-xc94",
        "sóc-trăng": "soc-trang-xc94",
        "sóc trăng": "soc-trang-xc94",
        "tay-ninh": "tay-ninh-xc72",
        "tây-ninh": "tay-ninh-xc72",
        "tây ninh": "tay-ninh-xc72",
        "tien-giang": "tien-giang-xc82",
        "tiền-giang": "tien-giang-xc82",
        "tiền giang": "tien-giang-xc82",
        "tra-vinh": "tra-vinh-xc84",
        "trà-vinh": "tra-vinh-xc84",
        "trà vinh": "tra-vinh-xc84",
        "vinh-long": "vinh-long-xc86",
        "vĩnh-long": "vinh-long-xc86",
        "vĩnh long": "vinh-long-xc86"
    }
    city_code = city_map.get(city_slug, "")

    if city_code:
        base = f"https://nhadat247.com.vn/{action}-nha-dat-{city_code}-{category_code}.html"
    else:
        base = f"https://nhadat247.com.vn/{action}-nha-dat-{category_code}.html"

    context.log.info(f"🔗 Generated URL: {base} for city={city}, propertyType={propType}, rentOrBuy={rentOrBuy}")
    
    # hiện tại chỉ trả 1 trang, dùng max_pages nếu cần mở rộng
    return [base]


@op(description="Run requests-based scraping and return DataFrame", out=Out(io_manager_key="fs_io_manager"))
def requests_scraping_op(context, urls: List[str], search_criteria: Dict) -> pd.DataFrame:
    """
    Stable requests-based scraping without Selenium DNS issues.
    """
    # Get config for production scaling - handle None case
    limit_each_page = 5  # Default value
    if context.op_config and isinstance(context.op_config, dict):
        limit_each_page = context.op_config.get("limit_each_page", 5)

    context.log.info(f"🚀 Starting requests scraping with {len(urls)} URLs, limit_each_page={limit_each_page}...")
    context.log.info(f"🔍 Search criteria type: {type(search_criteria)}")
    context.log.info(f"🔍 Search criteria: {search_criteria}")

    # Convert SearchCoordinate to dict if needed
    if hasattr(search_criteria, '_asdict'):  # NamedTuple-like
        search_dict = search_criteria._asdict()
    elif hasattr(search_criteria, '__dict__'):  # Object with attributes
        search_dict = search_criteria.__dict__
    elif isinstance(search_criteria, dict):
        search_dict = search_criteria
    else:
        # Try to access as dict-like
        search_dict = dict(search_criteria) if hasattr(search_criteria, 'keys') else {}

    search_city = search_dict.get("city", "")
    search_property_type = search_dict.get("propertyType", "")
    search_rent_or_buy = search_dict.get("rentOrBuy", "")
    search_radius = search_dict.get("radius", 0)

    context.log.info(f"🔍 Extracted - city: {search_city}, propertyType: {search_property_type}, rentOrBuy: {search_rent_or_buy}, radius: {search_radius}")

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

    # Add search criteria info to the DataFrame
    df["search_city"] = search_city
    df["search_property_type"] = search_property_type
    df["search_rent_or_buy"] = search_rent_or_buy
    df["search_radius"] = search_radius

    context.log.info(f"✅ Scraped {len(df)} properties for city: {search_city}.")
    return df


@op(description="Collects Search Criteria and create dynamic outputs", out=DynamicOut(io_manager_key="fs_io_manager"))
def collect_search_criterias(context, search_criterias: List[SearchCoordinate]):
    for search in search_criterias:
        # Convert SearchCoordinate to dict if needed
        if hasattr(search, '_asdict'):  # NamedTuple-like
            search_dict = search._asdict()
        elif hasattr(search, '__dict__'):  # Object with attributes
            search_dict = search.__dict__
        elif isinstance(search, dict):
            search_dict = search
        else:
            # Try to access as dict-like
            search_dict = dict(search) if hasattr(search, 'keys') else {}

        city = search_dict.get("city", "")
        rent_or_buy = search_dict.get("rentOrBuy", "")
        property_type = search_dict.get("propertyType", "")
        radius = search_dict.get("radius", 0)

        key = (
            "_".join(
                [
                    city,
                    rent_or_buy,
                    property_type,
                    str(radius),
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
    return requests_scraping_op(urls, search_criteria)


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
    search_criterias = collect_search_criterias()
    scrape_results = search_criterias.map(requests_scrape_properties)
    merge = merge_staging_to_delta_table_composite.alias("merge_staging_to_delta_table")
    merged_data = merge(collect_properties(scrape_results.collect()))

    # Export to PostgreSQL after merging - truyền tất cả search criterias
    export_result = export_to_postgres_op(merged_data, search_criterias.collect())

    # Run data exploration
    data_exploration(merged_data)
