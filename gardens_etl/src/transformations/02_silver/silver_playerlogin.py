import dlt
from pyspark.sql.functions import (
    from_unixtime,
    col, count,
    get_json_object
)

# Step 1: Create a temporary view - does the silver transformations
@dlt.view
def silver_playerlogin_view():
    catalog = spark.conf.get("catalog")
    return (
        spark.readStream.table(f"{catalog}.1_bronze.bronze_events")
        .select(
            from_unixtime(col("Timestampmillis") / 1000).alias("EventTimestamp"),
            get_json_object(col("Eventjson"), "$.playerId").alias("PlayerId"),
            get_json_object(col("Eventjson"), "$.socialId").alias("SocialId"),
            get_json_object(col("Eventjson"), "$.sessionId").alias("SessionId"),
            get_json_object(col("Eventjson"), "$.idProvider").alias("IdProvider"),
            col("Eventjson")
        )
    )

# Step 2. Aggregation table with expectation
@dlt.table(
    name=f"{spark.conf.get('catalog')}.2_silver.playerlogin_validation",
    comment="Aggregated player login counts"
)
@dlt.expect_or_fail("positive_login_count", "login_count > 0")
def playerlogin_validation():
    return (
        dlt.read_stream("silver_playerlogin_view")
        .groupBy("PlayerId")
        .agg(count("*").alias("login_count"))
    )


# Step 3: Create a streaming table from the view - dependent on the validation table
@dlt.table(
    name=f"{spark.conf.get('catalog')}.2_silver.silver_playerlogin",
    comment="Silver player login streaming table",
    table_properties={
        "quality": "silver"
    }
)
@dlt.expect_or_drop("valid_player_id", "PlayerId IS NOT NULL AND PlayerId != ''")
def silver_playerlogin():
    v = dlt.read_stream("playerlogin_validation")  # makes tabel dependent on validation
    return dlt.read_stream("silver_playerlogin_view")
