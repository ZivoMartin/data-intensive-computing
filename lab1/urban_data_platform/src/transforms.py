from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, TimestampType

NY_TZ = "America/New_York"


def transform_taxi_trips(df: DataFrame) -> DataFrame:
    df = (
        df.withColumn("pickup_timestamp_local", F.to_timestamp("tpep_pickup_datetime"))
        .withColumn("dropoff_timestamp_local", F.to_timestamp("tpep_dropoff_datetime"))
        .withColumn(
            "pickup_timestamp_utc",
            F.to_utc_timestamp(F.col("pickup_timestamp_local"), NY_TZ),
        )
        .withColumn(
            "dropoff_timestamp_utc",
            F.to_utc_timestamp(F.col("dropoff_timestamp_local"), NY_TZ),
        )
        .withColumnRenamed("pulocationid", "pulocation_id")
        .withColumnRenamed("dolocationid", "dolocation_id")
    )

    df = df.withColumn("pulocation_id", F.col("pulocation_id").cast(IntegerType()))
    df = df.withColumn("dolocation_id", F.col("dolocation_id").cast(IntegerType()))

    df = df.withColumn(
        "trip_duration_seconds",
        F.col("dropoff_timestamp_utc").cast("long") - F.col("pickup_timestamp_utc").cast("long"),
    )

    df = df.withColumn("pickup_hour_utc", F.date_trunc("hour", F.col("pickup_timestamp_utc")))
    df = df.withColumn("pickup_date", F.to_date(F.col("pickup_timestamp_utc")))
    df = df.withColumn("pickup_year", F.year(F.col("pickup_timestamp_utc")))
    df = df.withColumn("pickup_month", F.month(F.col("pickup_timestamp_utc")))

    hash_input = F.concat_ws(
        "|",
        *[F.coalesce(F.col(c).cast("string"), F.lit("")) for c in df.columns],
    )
    df = df.withColumn("trip_id", F.sha2(hash_input, 256))

    return df


def transform_weather(df: DataFrame) -> DataFrame:
    date_part = F.concat_ws(
        "-",
        F.col("year").cast("string"),
        F.lpad(F.col("month").cast("string"), 2, "0"),
        F.lpad(F.col("day").cast("string"), 2, "0"),
    )
    time_part = F.concat(F.lpad(F.col("hour").cast("string"), 2, "0"), F.lit(":00:00"))
    df = df.withColumn(
        "observation_timestamp_local",
        F.to_timestamp(F.concat_ws(" ", date_part, time_part)),
    )
    df = df.withColumn(
        "observation_timestamp_utc",
        F.to_utc_timestamp(F.col("observation_timestamp_local"), NY_TZ),
    )
    df = df.withColumn("observation_hour_utc", F.date_trunc("hour", F.col("observation_timestamp_utc")))

    numeric_cols = [
        "temp", "rhum", "prcp", "snwd", "wdir", "wspd", "wpgt", "pres", "cldc",
    ]
    for c in numeric_cols:
        if c in df.columns:
            df = df.withColumn(c, F.col(c).cast(DoubleType()))

    return df


def transform_air_quality(df: DataFrame) -> DataFrame:
    df = df.withColumn(
        "observation_timestamp_local",
        F.to_timestamp(F.concat_ws(" ", F.col("date_local"), F.col("time_local"))),
    )
    df = df.withColumn(
        "observation_timestamp_utc",
        F.to_utc_timestamp(F.col("observation_timestamp_local"), NY_TZ),
    )
    df = df.withColumn("observation_hour_utc", F.date_trunc("hour", F.col("observation_timestamp_utc")))
    df = df.withColumn("observation_year", F.year(F.col("observation_timestamp_utc")))
    df = df.withColumn("observation_month", F.month(F.col("observation_timestamp_utc")))
    df = df.withColumn("sample_measurement", F.col("sample_measurement").cast(DoubleType()))
    df = df.withColumn("latitude", F.col("latitude").cast(DoubleType()))
    df = df.withColumn("longitude", F.col("longitude").cast(DoubleType()))
    return df


def transform_taxi_zones(df: DataFrame) -> DataFrame:
    df = df.withColumn("location_id", F.col("location_id").cast(IntegerType()))
    return df
