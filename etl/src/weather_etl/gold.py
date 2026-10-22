"""silver.weather_hourly -> gold analytics tables (full rebuild each run; all are small)."""

from __future__ import annotations

from collections.abc import Callable
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from weather_etl.paths import table_path

GOLD_TABLES = (
    "daily_city",
    "climatology_doy",
    "anomaly_monthly",
    "anomaly_annual",
    "trend_city",
    "extremes_annual",
    "distribution_decade",
)


def _in_baseline(b0: int, b1: int):
    return F.col("year").between(b0, b1)


def daily_city(silver: DataFrame) -> DataFrame:
    return (
        silver.groupBy("city_id", F.col("date_local").alias("date"))
        .agg(
            F.min("temperature_2m").alias("tmin"),
            F.avg("temperature_2m").alias("tmean"),
            F.max("temperature_2m").alias("tmax"),
            F.sum("precipitation").alias("precip_mm"),
            F.sum("snowfall").alias("snow_cm"),
            F.max("wind_gusts_10m").alias("gust_max"),
            F.avg("relative_humidity_2m").alias("rh_mean"),
            F.count("*").alias("hours"),
        )
        .filter(F.col("hours") >= 23)  # DST days have 23 or 25 local hours
        .withColumn("year", F.year("date"))
        .withColumn("month", F.month("date"))
        .withColumn("doy", F.dayofyear("date"))
    )


def climatology_doy(daily: DataFrame, b0: int, b1: int) -> DataFrame:
    spread = (
        daily.filter(_in_baseline(b0, b1))
        .select("city_id", "tmean", "doy", F.explode(F.sequence(F.lit(-7), F.lit(7))).alias("off"))
        .select(
            "city_id",
            "tmean",
            (F.pmod(F.col("doy") - 1 + F.col("off"), F.lit(366)) + 1).alias("doy"),
        )
    )
    return (
        spread.groupBy("city_id", "doy")
        .agg(F.expr("percentile(tmean, array(0.1, 0.5, 0.9))").alias("p"), F.count("*").alias("n"))
        .select(
            "city_id",
            "doy",
            F.col("p")[0].alias("p10"),
            F.col("p")[1].alias("p50"),
            F.col("p")[2].alias("p90"),
            "n",
        )
    )


def anomaly_monthly(daily: DataFrame, b0: int, b1: int) -> DataFrame:
    days_in_month = F.dayofmonth(F.last_day(F.make_date("year", "month", F.lit(1))))
    monthly = (
        daily.groupBy("city_id", "year", "month")
        .agg(F.avg("tmean").alias("tmean"), F.count("*").alias("days"))
        .filter(F.col("days") == days_in_month)
    )
    base = (
        monthly.filter(_in_baseline(b0, b1))
        .groupBy("city_id", "month")
        .agg(F.avg("tmean").alias("tmean_base"))
    )
    return (
        monthly.join(base, ["city_id", "month"])
        .withColumn("anomaly", F.col("tmean") - F.col("tmean_base"))
        .select("city_id", "year", "month", "tmean", "anomaly")
    )


def anomaly_annual(daily: DataFrame, b0: int, b1: int) -> DataFrame:
    days_in_year = F.dayofyear(F.make_date("year", F.lit(12), F.lit(31)))
    annual = (
        daily.groupBy("city_id", "year")
        .agg(
            F.avg("tmean").alias("tmean"),
            F.sum("precip_mm").alias("precip_mm"),
            F.count("*").alias("days"),
        )
        .filter(F.col("days") == days_in_year)
    )
    base = (
        annual.filter(_in_baseline(b0, b1))
        .groupBy("city_id")
        .agg(F.avg("tmean").alias("tmean_base"))
    )
    return (
        annual.join(base, "city_id")
        .withColumn("anomaly", F.col("tmean") - F.col("tmean_base"))
        .select("city_id", "year", "tmean", "anomaly", "precip_mm")
    )


def trend_city(annual: DataFrame) -> DataFrame:
    g = annual.groupBy("city_id").agg(
        (F.covar_pop("year", "anomaly") / F.var_pop("year")).alias("slope"),
        F.avg("year").alias("mean_year"),
        F.avg("anomaly").alias("mean_anomaly"),
        F.corr("year", "anomaly").alias("r"),
        F.count("*").alias("n_years"),
    )
    return g.filter(F.col("n_years") >= 2).select(
        "city_id",
        (F.col("slope") * 10).alias("slope_c_per_decade"),
        (F.col("mean_anomaly") - F.col("slope") * F.col("mean_year")).alias("intercept"),
        (F.col("r") * F.col("r")).alias("r2"),
        "n_years",
    )


def extremes_annual(daily: DataFrame, b0: int, b1: int) -> DataFrame:
    thresholds = (
        daily.filter(_in_baseline(b0, b1))
        .groupBy("city_id")
        .agg(
            F.expr("percentile(tmax, 0.95)").alias("tmax_p95"),
            F.expr("percentile(tmin, 0.05)").alias("tmin_p05"),
            F.expr("percentile(IF(precip_mm >= 1, precip_mm, NULL), 0.95)").alias("wet_p95"),
        )
    )

    def count(cond):
        return F.coalesce(F.sum(cond.cast("int")), F.lit(0))

    return (
        daily.join(thresholds, "city_id")
        .groupBy("city_id", "year")
        .agg(
            count(F.col("tmax") > F.col("tmax_p95")).alias("hot_days"),
            count(F.col("tmin") < F.col("tmin_p05")).alias("cold_nights"),
            count(F.col("precip_mm") > F.col("wet_p95")).alias("heavy_rain_days"),
            F.max("gust_max").alias("gust_max"),
        )
    )


def distribution_decade(daily: DataFrame) -> DataFrame:
    return (
        daily.withColumn("decade", (F.floor(F.col("year") / 10) * 10).cast("int"))
        .withColumn("bin", F.floor(F.col("tmean") / 0.5) * 0.5)
        .groupBy("city_id", "decade", "bin")
        .agg(F.count("*").alias("days"))
    )


def build_gold(
    spark: SparkSession,
    root: Path,
    baseline_start: int,
    baseline_end: int,
    check: Callable[[dict[str, int]], None] | None = None,
    first_year: int | None = None,
) -> dict[str, int]:
    """Rebuild every gold table. `check` sees the row counts first and may raise: nothing is
    overwritten unless it passes, so a bad run never replaces the dashboard's data."""
    silver = spark.read.format("delta").load(table_path(root, "silver", "weather_hourly"))
    daily = daily_city(silver)
    if first_year is not None:  # silver holds Dec 31 before it: local Jan 1 east of UTC
        daily = daily.filter(F.col("year") >= first_year)
    daily = daily.cache()
    annual = anomaly_annual(daily, baseline_start, baseline_end).cache()
    frames = {
        "daily_city": daily,
        "climatology_doy": climatology_doy(daily, baseline_start, baseline_end),
        "anomaly_monthly": anomaly_monthly(daily, baseline_start, baseline_end),
        "anomaly_annual": annual,
        "trend_city": trend_city(annual),
        "extremes_annual": extremes_annual(daily, baseline_start, baseline_end),
        "distribution_decade": distribution_decade(daily),
    }
    try:
        counts = {name: df.count() for name, df in frames.items()}
        if check is not None:
            check(counts)
        for name, df in frames.items():
            (
                df.coalesce(1)
                .write.format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .save(table_path(root, "gold", name))
            )
    finally:
        annual.unpersist()
        daily.unpersist()
    return counts
