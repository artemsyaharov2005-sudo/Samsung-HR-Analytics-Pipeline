
import os
import time
import socket
import pandas as pd
import matplotlib.pyplot as plt

from pyspark.sql import SparkSession, functions as F


STAGE_CSV = os.path.join("data", "stage", "vacancies_stage.csv")
OUT_DIR = os.path.join("data", "reports")
OUT_CSV = os.path.join(OUT_DIR, "scalability_results.csv")
OUT_PLOT = os.path.join(OUT_DIR, "scalability_plot.png")

MULTIPLIERS = [1, 2, 4, 8, 16]
REPEATS = 3

APP_NAME = "Vacancies Scalability Benchmark (Win Fix)"



def setup_windows_hadoop():
    """
    If project has ./hadoop/bin/winutils.exe, set HADOOP_HOME so Spark stops warning.
    """
    hadoop_dir = os.path.abspath("hadoop")
    winutils = os.path.join(hadoop_dir, "bin", "winutils.exe")
    if os.path.exists(winutils):
        os.environ["HADOOP_HOME"] = hadoop_dir
        os.environ["hadoop.home.dir"] = hadoop_dir
        print(f"✅ Найден winutils.exe, HADOOP_HOME = {hadoop_dir}")
    else:
        print("⚠️ winutils.exe не найден (можно игнорировать, но лучше добавить ./hadoop/bin/winutils.exe)")


def force_localhost_env():
    """
    Fix Spark 4.x 'Invalid Spark URL' on Windows where hostname may contain '_' etc.
    Force driver to bind to localhost/127.0.0.1.
    """
    os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
    os.environ.setdefault("PYSPARK_PYTHON", sys_executable())
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys_executable())


def sys_executable():

    return os.environ.get("VIRTUAL_ENV") and os.path.join(os.environ["VIRTUAL_ENV"], "Scripts", "python.exe") or "python"


def make_spark():
    """
    Create SparkSession with Windows-safe networking settings.
    """
    return (
        SparkSession.builder
        .appName(APP_NAME)
        .master("local[*]")

        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.local.ip", "127.0.0.1")

        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )



def ensure_dirs():
    os.makedirs(OUT_DIR, exist_ok=True)


def safe_to_double(colname: str):
    s = F.col(colname).cast("string")
    cleaned = F.regexp_replace(s, r"[^0-9\-\.,]+", "")
    token = F.regexp_extract(cleaned, r"(-?\d+(?:[.,]\d+)?)", 1)
    token = F.when(F.length(token) > 0, token).otherwise(F.lit(None))
    token = F.regexp_replace(token, ",", ".")
    return token.cast("double")


def add_salary_mid(df):
    sf = safe_to_double("salary_from") if "salary_from" in df.columns else F.lit(None).cast("double")
    st = safe_to_double("salary_to") if "salary_to" in df.columns else F.lit(None).cast("double")

    salary_mid = (
        F.when(sf.isNotNull() & st.isNotNull(), (sf + st) / F.lit(2.0))
        .when(sf.isNotNull(), sf)
        .when(st.isNotNull(), st)
        .otherwise(F.lit(None).cast("double"))
    )
    return df.withColumn("salary_mid", salary_mid)


def build_scaled_df(base_df, m: int):
    if m <= 1:
        return base_df
    reps = F.array(*[F.lit(i) for i in range(m)])
    return base_df.withColumn("_rep", F.explode(reps)).drop("_rep")


def heavy_query(df):
    """
    Heavier query to show scaling:
    - filter salary_mid not null
    - multiple aggs + joins + percentile approx + orderBy
    """
    if "area_name" not in df.columns:
        raise ValueError("Column 'area_name' is required for benchmark.")

    d = df.filter(F.col("salary_mid").isNotNull())

    # agg salary
    agg_salary = (
        d.groupBy("area_name")
         .agg(
            F.avg("salary_mid").alias("avg_salary"),
            F.expr("percentile_approx(salary_mid, 0.5)").alias("median_salary")
         )
    )

    # agg count
    if "id" in d.columns:
        agg_cnt = (
            d.groupBy("area_name")
             .agg(
                F.count(F.lit(1)).alias("vac_count"),
                F.countDistinct("id").alias("vac_distinct")
             )
        )
    else:
        agg_cnt = d.groupBy("area_name").agg(F.count(F.lit(1)).alias("vac_count")).withColumn("vac_distinct", F.lit(None))

    # optional test share
    if "test_required" in d.columns:
        test_bin = F.when(F.col("test_required").isin(True, 1, "1", "true", "True", "TRUE"), 1) \
                    .when(F.col("test_required").isin(False, 0, "0", "false", "False", "FALSE"), 0) \
                    .otherwise(None)
        agg_test = d.withColumn("test_bin", test_bin.cast("double")) \
                    .groupBy("area_name") \
                    .agg(F.avg("test_bin").alias("test_share"))
    else:
        agg_test = d.groupBy("area_name").agg(F.lit(None).cast("double").alias("test_share"))

    res = (
        agg_salary.join(agg_cnt, "area_name", "inner")
                  .join(agg_test, "area_name", "left")
                  .orderBy(F.col("vac_count").desc())
    )
    return res


def time_count(df):
    t0 = time.time()
    df.count()
    return time.time() - t0



def main():
    ensure_dirs()
    setup_windows_hadoop()
    force_localhost_env()

    spark = make_spark()
    spark.sparkContext.setLogLevel("WARN")

    try:
        if not os.path.exists(STAGE_CSV):
            raise FileNotFoundError(f"Stage CSV not found: {os.path.abspath(STAGE_CSV)}")

        print(f"✅ Spark: {spark.version}")
        print(f"📥 Reading stage CSV: {os.path.abspath(STAGE_CSV)}")

        base_df = (
            spark.read
                 .option("header", True)
                 .option("inferSchema", True)
                 .csv(STAGE_CSV)
        )

        base_df = add_salary_mid(base_df).repartition(4)

        # Global warm-up
        print("🔥 Global warm-up...")
        heavy_query(base_df).count()

        results = []

        for m in MULTIPLIERS:
            df_scaled = build_scaled_df(base_df, m)

            # warm-up per multiplier
            heavy_query(df_scaled).count()

            # cache to reduce variability
            df_scaled = df_scaled.cache()
            df_scaled.count()

            times = []
            for _ in range(REPEATS):
                times.append(time_count(heavy_query(df_scaled)))

            rows = df_scaled.count()
            df_scaled.unpersist()

            rec = {
                "multiplier": m,
                "rows": int(rows),
                "min_time_s": float(min(times)),
                "max_time_s": float(max(times)),
                "avg_time_s": float(sum(times) / len(times)),
            }
            results.append(rec)

            print(
                f"m={m:<2} rows={rec['rows']:<9} "
                f"min={rec['min_time_s']:.3f}s avg={rec['avg_time_s']:.3f}s max={rec['max_time_s']:.3f}s"
            )

        df_res = pd.DataFrame(results)
        df_res.to_csv(OUT_CSV, index=False)
        print(f"✅ Results CSV: {os.path.abspath(OUT_CSV)}")

        plt.figure(figsize=(9, 5))
        plt.plot(df_res["rows"], df_res["avg_time_s"], marker="o")
        plt.xlabel("Rows")
        plt.ylabel("Avg time (s)")
        plt.title("Scalability: avg execution time vs data volume")
        plt.grid(True)
        plt.tight_layout()
        plt.savefig(OUT_PLOT, dpi=150)
        plt.close()
        print(f"✅ Plot: {os.path.abspath(OUT_PLOT)}")

    finally:
        spark.stop()
        print("🛑 Spark остановлен")


if __name__ == "__main__":
    main()
