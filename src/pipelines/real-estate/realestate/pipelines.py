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
    # map nếu cần (bổ sung)
    city_map = {"hanoi": "ha-noi-xc1", "ho-chi-minh": "ho-chi-minh-xc79"}
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
    data_exploration(merge(collect_properties(search_criterias.collect())))
