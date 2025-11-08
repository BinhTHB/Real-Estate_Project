# gernal op pyspark execution
import json

from .types import S3Coordinate

from pandas import DataFrame


from realestate.common.types_realestate import PropertyDataFrame
from realestate.common.helper_functions import reading_delta_table

import re
import os

import pandas as pd
import pandasql as ps
import pyarrow as pa

from dagster import (
    LocalFileHandle,
    op,
    Field,
    String,
    Output,
    Out,
    In,
    check,
)

from realestate.common.types import DeltaCoordinate
from realestate.common.helper_functions import rename_pandas_dataframe_columns, read_gzipped_json

from dagster import Field, String


PARQUET_SPECIAL_CHARACTERS = r"[ ,;{}()\n\t=]"


def _get_s3a_path(bucket, path):
    # TODO: remove unnessesary slashs if there
    return "s3a://" + bucket + "/" + path


@op(
    required_resource_keys={"pyspark", "s3"},
    description="""Ingest s3 path with zipped jsons
and load it into a Spark Dataframe.
It infers header names but and infer schema.

It also ensures that the column names are valid parquet column names by
filtering out any of the following characters from column names:

Characters (within quotations): "`{chars}`"

""".format(
        chars=PARQUET_SPECIAL_CHARACTERS
    ),
)
def s3_to_df(context, s3_coordinate: S3Coordinate) -> DataFrame:
    context.log.debug(
        "AWS_KEY: {access} - Secret: {secret})".format(
            access=os.environ["MINIO_ROOT_USER"], secret=os.environ["MINIO_ROOT_PASSWORD"]
        )
    )
    # findspark.init(spark_home='/path/to/spark/lib/')
    s3_path = _get_s3a_path(s3_coordinate["bucket"], s3_coordinate["key"])

    context.log.info(
        "Reading dataframe from s3 path: {path} (Bucket: {bucket} and Key: {key})".format(
            path=s3_path, bucket=s3_coordinate["bucket"], key=s3_coordinate["key"]
        )
    )

    # reading from a folder handles zipped and unzipped jsons automatically
    data_frame = context.resources.pyspark.spark_session.read.json(s3_path)

    # df.columns #print columns

    context.log.info("Column FactId removed from df")

    # parquet compat
    return rename_spark_dataframe_columns(
        data_frame, lambda x: re.sub(PARQUET_SPECIAL_CHARACTERS, "", x)
    )


@op(
    description="""This function is to flatten the nested json properties to a table with flat columns. Renames columns to avoid parquet special characters.""",
    config_schema={
        "remove_columns": Field(
            [String],
            default_value=[
                "propertyDetails_images",
                "propertyDetails_pdfs",
                "propertyDetails_commuteTimes_defaultPois_transportations",
                "viewData_viewDataWeb_webView_structuredData",
            ],
            is_required=False,
            description=("unessesary columns to be removed in from the json"),
        ),
    },
    out=Out(io_manager_key="fs_io_manager"),
)
def flatten_json(context, local_file: LocalFileHandle) -> pd.DataFrame:

    # reading from a folder with zipped JSONs 
    context.log.info(f"Reading from local file: {local_file.path} ...")
    json_data = read_gzipped_json(local_file.path)

    # Flatten: Normalize the JSON data
    # df = pd.json_normalize(json_data) # gốc
    df = pd.json_normalize(json_data, sep='_', max_level=None) 

    context.log.info(f"Original flattened columns: {df.columns.tolist()}")
    context.log.info(f"Flattened columns count: {len(df.columns)}")

    #TODO: Still need to remove FactId?
    if 'FactId' in df.columns:
        df.drop('FactId', axis=1, inplace=True)
        context.log.info("Column FactId removed from df")


    # rename for avoid parquet special characters
    df = rename_pandas_dataframe_columns(
        df, lambda x: re.sub(PARQUET_SPECIAL_CHARACTERS, "", x)
    )

    # df.drop(columns=context.op_config["remove_columns"], errors='ignore', inplace=True)
    # convert . column names to underlines
    df.columns = df.columns.str.replace('.', '_', regex=False)
    
    # Rename price column for consistency
    if 'listing_prices_buy_price' in df.columns:
        df.rename(columns={'listing_prices_buy_price': 'propertyDetails_normalizedPrice'}, inplace=True)
        context.log.info("Renamed column 'listing_prices_buy_price' to 'propertyDetails_normalizedPrice'")
    elif 'listing_prices_rent_price' in df.columns:
        df.rename(columns={'listing_prices_rent_price': 'propertyDetails_normalizedPrice'}, inplace=True)
        context.log.info("Renamed column 'listing_prices_rent_price' to 'propertyDetails_normalizedPrice'")
        
        # 🧩 Kiểm tra cột chứa list
    list_cols = [c for c in df.columns if df[c].apply(lambda v: isinstance(v, list)).any()]
    if list_cols:
        context.log.warning(f"Columns still contain lists: {list_cols[:5]} ...")
        # Có thể chuyển list sang JSON string để lưu an toàn
        for c in list_cols:
            df[c] = df[c].apply(lambda x: json.dumps(x) if isinstance(x, list) else x)

    context.log.info(f"Flattened df shape: {df.shape}")
    context.log.info(f"Flattened df columns preview: {df.columns.tolist()}")
    context.log.info(f"Sample row: {df.head(1).to_dict(orient='records')}")
    
    context.log.info(f"faltten df length length: {len(df)} and schema: {df.columns}")
    return df



# @op(out=Out(io_manager_key="fs_io_manager"))
# def merge_property_delta(context, input_dataframe: DataFrame) -> DeltaCoordinate:
    
#     target_delta_table = "s3a://real-estate/lake/bronze/property"
#     target_delta_coordinate = { "s3_coordinate_bucket": "real-estate", "s3_coordinate_key": "lake/bronze/property", "table_name": "property", "database": "core"}

#     df, dt = reading_delta_table(context, target_delta_table)

#     input_table_pa = pa.Table.from_pandas(input_dataframe)

#     # # Enhanced logging for debugging
#     # context.log.info(f"Input dataframe columns: {input_dataframe.columns.to_list()}")
#     # context.log.info(f"Input dataframe schema: {input_table_pa.schema}")
#     # context.log.info(f"Target Delta table schema: {dt.to_pyarrow_dataset().schema}")

#     # # Check if merge key exists in input dataframe
#     # if 'propertyDetails_propertyId' not in input_dataframe.columns:
#     #     context.log.error("Merge key 'propertyDetails_propertyId' not found in the input dataframe!")
#     #     raise ValueError("Merge key 'propertyDetails_propertyId' not found in the input dataframe")

#     context.log.debug(f"Target Delta table schema: {dt.to_pyarrow_dataset().schema}")
#     context.log.debug(f"input_dataframe: {type(input_dataframe)} and lenght {len(input_dataframe)}")
#     context.log.debug(f"input_dataframe schema: {input_table_pa.schema}")

#     (
#         dt.merge(
#             source=input_dataframe,
#             # predicate='target.propertyDetails_id = source."propertyDetails_propertyId"',
#             predicate='target.propertyDetails_propertyId = source.propertyDetails_propertyId',
#             source_alias='source',
#             target_alias='target')
#         .when_matched_update_all()
#         .when_not_matched_insert_all()
#         .execute()
#     )
#     context.log.info("Merged data into Delta table `property` successfully")

#     context.log.info(f"📋 Input DataFrame columns: {len(input_dataframe.columns)}")
#     context.log.info(f"📋 Delta Table before merge columns: {len(dt.to_pyarrow_dataset().schema)}")

#     #return delta coordinates for notebooks to read from
#     return target_delta_coordinate

@op(ins={"input_dataframe": In(dagster_type=PropertyDataFrame)}, out=Out(io_manager_key="fs_io_manager"))
def merge_property_delta(context, input_dataframe) -> DeltaCoordinate:
    import pyarrow as pa
    from deltalake import DeltaTable, write_deltalake
    
    # Convert list to DataFrame if necessary
    if isinstance(input_dataframe, list):
        input_dataframe = pd.DataFrame(input_dataframe)
    
    print("📋 Các cột hiện có:", input_dataframe.columns.tolist())
    print("📄 5 dòng đầu:\n", input_dataframe.head())

    # 🧩 Đảm bảo cột khóa tồn tại cho merge
    if "propertyDetails_propertyId" not in input_dataframe.columns:
        if "id" in input_dataframe.columns:
            input_dataframe["propertyDetails_propertyId"] = input_dataframe["id"].astype(str)
        else:
            # Create id from url hash if id column doesn't exist
            import hashlib
            input_dataframe["propertyDetails_propertyId"] = input_dataframe["url"].apply(lambda x: hashlib.md5(str(x).encode()).hexdigest()[:16])
        context.log.warning("🧩 Added column propertyDetails_propertyId for merge key")

    target_delta_table = "s3a://real-estate/lake/bronze/property"
    target_delta_coordinate = {
        "s3_coordinate_bucket": "real-estate",
        "s3_coordinate_key": "lake/bronze/property",
        "table_name": "property",
        "database": "core"
    }

    storage_options = {
        "AWS_ACCESS_KEY_ID": "minioadmin",
        "AWS_SECRET_ACCESS_KEY": "minioadmin",
        "AWS_ENDPOINT_URL": "http://127.0.0.1:9000",
        "AWS_REGION": "us-east-1",
        "AWS_ALLOW_HTTP": "true",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true"  # <-- THÊM DÒNG NÀY
    }

    # Đọc Delta Table (nếu có)
    try:
        df, dt = reading_delta_table(context, target_delta_table)
    except Exception as e:
        context.log.warning(f"Delta table not found, will create new one: {e}")
        df, dt = None, None # (Sau khi bạn xóa Bảng ở Bước 1, code sẽ đi vào đây)

    context.log.info(f"📋 Input DataFrame columns: {len(input_dataframe.columns)}")

    # Chuyển đổi sang PyArrow MỘT LẦN ở đầu
    try:
        pyarrow_table = pa.Table.from_pandas(input_dataframe)
    except Exception as e:
        context.log.error(f"Lỗi khi chuyển đổi Pandas sang PyArrow: {e}")
        raise e

    # Nếu bảng Delta chưa tồn tại — tạo mới
    # (Sau Bước 1, code sẽ chạy khối này)
    if dt is None:
        context.log.warning("🆕 Creating new Delta table in MinIO ...")

        write_deltalake(
            target_delta_table,
            pyarrow_table,  # Dùng PyArrow Table
            mode="overwrite",
            overwrite_schema=True,
            engine="rust", # Dùng engine rust
            storage_options=storage_options
        )
        context.log.info("✅ Created new Delta table with full schema.")
        return target_delta_coordinate # Kết thúc op sau khi tạo bảng

    # XÓA BỎ KHỐI "ÉP" SCHEMA (KHỐI GÂY LỖI)
    # (Từ các lần chạy sau, code sẽ bỏ qua `if dt is None` và đi thẳng tới đây)

    context.log.info(f"📋 Delta Table before merge columns: {len(dt.to_pyarrow_dataset().schema)}")

    # Kiểm tra schema compatibility
    existing_schema = dt.to_pyarrow_dataset().schema
    new_schema = pyarrow_table.schema

    if len(existing_schema) != len(new_schema):
        context.log.warning(f"⚠️ Schema mismatch detected! Existing: {len(existing_schema)} cols, New: {len(new_schema)} cols")
        context.log.warning("🔄 Overwriting table with new schema...")

        # Force overwrite với schema mới
        write_deltalake(
            target_delta_table,
            pyarrow_table,
            mode="overwrite",
            overwrite_schema=True,
            engine="rust",
            storage_options=storage_options
        )
        context.log.info("✅ Table overwritten with new schema successfully.")
        return target_delta_coordinate

    # Schema compatible - thực hiện merge bình thường
    (
        dt.merge(
            source=pyarrow_table, # <-- Dùng PyArrow Table
            predicate="target.propertyDetails_propertyId = source.propertyDetails_propertyId",
            source_alias="source",
            target_alias="target",
        )
        .when_matched_update_all()
        .when_not_matched_insert_all()
        .execute()
    )

    context.log.info("✅ Merged data into Delta table `property` successfully.")

    dt.update_incremental()
    context.log.info(f"📋 Delta Table schema after merge: {len(dt.to_pyarrow_dataset().schema)}")

    return target_delta_coordinate

@op(
    required_resource_keys={"s3"},
    description="""This will check if property is already downloaded. If so, check if price or other
    columns have changed in the meantime, or if date is very old, download again""",
    out={"properties": Out(dagster_type=PropertyDataFrame, is_required=False, io_manager_key="fs_io_manager")},

)
def get_changed_or_new_properties(context, properties: PropertyDataFrame, property_table: pd.DataFrame) -> PropertyDataFrame:
    # prepare ids and fingerprints from fetched properties
    ids_tmp: list = [p["id"] for p in properties]
    ids: str = ", ".join(ids_tmp)

    context.log.info("Fetched propertyDetails_id's: [{}]".format(ids))
    # context.log.debug(f"type: property_table: {type(property_table)} and lenght {len(property_table)}")

    cols_props = ["propertyDetails_propertyId", "fingerprint"]
    cols_PropertyDataFrame = [
        "id",
        "fingerprint",
        "is_prefix",
        "rentOrBuy",
        "city",
        "propertyType",
        "radius",
        "last_normalized_price",
    ]

    query = f"""SELECT propertyDetails_propertyId
                , CAST(propertyDetails_propertyId AS STRING)
                    || '-'
                    || propertyDetails_normalizedPrice AS fingerprint
            FROM property_table
            WHERE propertyDetails_propertyId IN ( {ids} )
            """
    result_df = ps.sqldf(query, locals())
    context.log.info(f"Lenght: property_table: {len(result_df)}")

    # get a list selected colum: `property_ids` and its fingerprint
    existing_props = result_df[["propertyDetails_propertyId", "fingerprint"]].values.tolist()

    # Convert dict into pandas dataframe
    pd_existing_props = pd.DataFrame(existing_props, columns=cols_props)
    pd_properties = pd.DataFrame(properties, columns=cols_PropertyDataFrame)

    # debugging
    # context.log.debug(f"pd_existing_props: {pd_existing_props}, type: {type(pd_existing_props)}")
    # context.log.debug(f"pd_properties: {pd_properties}")

    # select new or changed once
    df_changed = ps.sqldf(
        """
        SELECT p.id, p.fingerprint, p.is_prefix, p.rentOrBuy, p.city, p.propertyType, p.radius, p.last_normalized_price
        FROM pd_properties p LEFT OUTER JOIN pd_existing_props e
            ON p.id = e.propertyDetails_propertyId
            WHERE p.fingerprint != e.fingerprint
                OR e.fingerprint IS NULL
        """, locals()
    )
    context.log.info(f"lenght: df_changed: {len(df_changed)}")
    if df_changed.empty:
        context.log.info("No property of [{}] changed".format(ids))
    else:
        changed_properties = []
        for index, row in df_changed.iterrows():
            changed_properties.append(row.to_dict())

        ids_changed = ", ".join(str(e) for e in df_changed["id"].tolist())

        context.log.info("changed properties: {}".format(ids_changed))
        yield Output(changed_properties, "properties")




#
# GENERAL MINOR SPARK FUNCTIONS
def do_prefix_column_names(df, prefix):
    check.inst_param(df, "df", DataFrame)
    check.str_param(prefix, "prefix")
    return rename_spark_dataframe_columns(
        df, lambda c: "{prefix}{c}".format(prefix=prefix, c=c)
    )


@op
def canonicalize_column_names(_context, data_frame: DataFrame) -> DataFrame:
    return rename_spark_dataframe_columns(data_frame, lambda c: c.lower())


def replace_values_spark(data_frame, old, new):
    return data_frame.na.replace(old, new)
