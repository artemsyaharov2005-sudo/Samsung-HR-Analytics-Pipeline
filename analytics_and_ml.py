# analytics_and_ml.py
# ------------------------------------------------------------
# Аналитика + ML-модель на Spark (Windows-friendly)
# Читает stage CSV, чистит зарплаты, сохраняет отчеты через pandas.
# Исправлено: безопасный test_flag (0/1), чтобы не падать на строках типа "Полный день".
# ML адаптирована под поля: area_id, salary_*, currency, address_lat.
# ------------------------------------------------------------

import os
import sys
from pathlib import Path


os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


if "SPARK_MASTER" in os.environ:
    print("🧹 Удаляю env SPARK_MASTER=" + os.environ["SPARK_MASTER"])
    os.environ.pop("SPARK_MASTER", None)


PROJECT_DIR = Path(__file__).resolve().parent
DATA_DIR = PROJECT_DIR / "data"
STAGE_CSV = DATA_DIR / "stage" / "vacancies_stage.csv"
REPORTS_DIR = DATA_DIR / "reports"
REPORTS_DIR.mkdir(parents=True, exist_ok=True)


def ensure_winutils():
    hadoop_home = PROJECT_DIR / "hadoop"
    winutils = hadoop_home / "bin" / "winutils.exe"
    if winutils.exists():
        os.environ["HADOOP_HOME"] = str(hadoop_home)
        os.environ["hadoop.home.dir"] = str(hadoop_home)
        print(f"✅ Найден winutils.exe, HADOOP_HOME = {hadoop_home}")
    else:
        print("⚠️ winutils.exe не найден (не критично)")


from pyspark.sql import SparkSession, functions as F

def make_spark(app_name="analytics_and_ml"):
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.network.timeout", "600s")
        .config("spark.executor.heartbeatInterval", "60s")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def save_small_df_via_pandas(sdf, out_csv: Path, max_rows=50000):
    pdf = sdf.limit(max_rows).toPandas()
    pdf.to_csv(out_csv, index=False, encoding="utf-8-sig")
    print(f"✅ Saved: {out_csv}")


def plot_bar_from_csv(csv_path: Path, x_col: str, y_col: str, out_png: Path, title: str):
    import pandas as pd
    import matplotlib.pyplot as plt

    df = pd.read_csv(csv_path)
    plt.figure(figsize=(10, 5))
    plt.bar(df[x_col].astype(str), df[y_col])
    plt.xticks(rotation=45, ha="right")
    plt.title(title)
    plt.tight_layout()
    plt.savefig(out_png, dpi=160)
    plt.close()
    print(f"✅ Plot: {out_png}")


def to_double_clean(colname: str):
    s = F.trim(F.col(colname).cast("string"))
    s = F.regexp_replace(s, r"[^0-9\.,\-]", "")
    s = F.regexp_replace(s, r",", ".")
    num = F.regexp_extract(s, r"(-?\d+(?:\.\d+)?)", 1)
    return F.when(num == "", F.lit(None)).otherwise(num.cast("double"))

def apply_salary_cleaning(df):
    out = df
    if "salary_from" in out.columns:
        out = out.withColumn("salary_from", to_double_clean("salary_from"))
    if "salary_to" in out.columns:
        out = out.withColumn("salary_to", to_double_clean("salary_to"))
    if "address_lat" in out.columns:
        out = out.withColumn("address_lat", to_double_clean("address_lat"))
    return out


def add_test_flag(df):
    # выбираем, из какой колонки брать (твоя — test_required)
    if "test_required" in df.columns:
        src = F.col("test_required")
    elif "has_test" in df.columns:
        src = F.col("has_test")
    else:
        return df.withColumn("test_flag", F.lit(0).cast("int"))

    s = F.lower(F.trim(src.cast("string")))

    test_flag = (
        F.when(src.cast("string").rlike(r"^\s*\d+\s*$"), src.cast("int"))  # "0"/"1"/"2"...
         .when(s.isin("true", "yes", "да", "y", "1"), F.lit(1))
         .when(s.isin("false", "no", "нет", "n", "0"), F.lit(0))
         # если пришла любая “мусорная” строка (типа "Полный день") — считаем как NULL -> потом coalesce в 0
         .otherwise(F.lit(None).cast("int"))
    )

    return df.withColumn("test_flag", F.coalesce(test_flag, F.lit(0)).cast("int"))


from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer, OneHotEncoder, VectorAssembler
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import BinaryClassificationEvaluator

def train_simple_model(df, reports_dir: Path):

    work = df


    if "salary_gross" in work.columns:
        sg = F.lower(F.trim(F.col("salary_gross").cast("string")))
        work = work.withColumn(
            "salary_gross_bin",
            F.when(sg.isin("true", "1", "yes", "да"), 1)
             .when(sg.isin("false", "0", "no", "нет"), 0)
             .otherwise(F.lit(0))
             .cast("int")
        )
    else:
        work = work.withColumn("salary_gross_bin", F.lit(0).cast("int"))


    if "area_id" in work.columns:
        area_num = F.when(F.col("area_id").cast("string").rlike(r"^\s*\d+\s*$"), F.col("area_id").cast("double")).otherwise(F.lit(0.0))
        work = work.withColumn("area_id_num", area_num)
    else:
        work = work.withColumn("area_id_num", F.lit(0.0))


    for c in ["salary_from", "salary_to", "address_lat"]:
        if c in work.columns:
            work = work.withColumn(c, F.coalesce(F.col(c), F.lit(0.0)))
        else:
            work = work.withColumn(c, F.lit(0.0))


    if "salary_currency" in work.columns:
        cur = F.upper(F.trim(F.col("salary_currency").cast("string")))
        work = work.withColumn("salary_currency_norm", F.when(cur == "", F.lit("NA")).otherwise(cur))
    else:
        work = work.withColumn("salary_currency_norm", F.lit("NA"))

    indexer = StringIndexer(inputCol="salary_currency_norm", outputCol="currency_idx", handleInvalid="keep")
    encoder = OneHotEncoder(inputCols=["currency_idx"], outputCols=["currency_ohe"], handleInvalid="keep")

    assembler = VectorAssembler(
        inputCols=["salary_from", "salary_to", "salary_gross_bin", "address_lat", "area_id_num", "currency_ohe"],
        outputCol="features"
    )

    lr = LogisticRegression(maxIter=30, regParam=0.01)

    pipeline = Pipeline(stages=[indexer, encoder, assembler, lr])

    data = work.select(
        F.col("test_flag").cast("int").alias("label"),
        "salary_from", "salary_to", "salary_gross_bin", "address_lat", "area_id_num", "salary_currency_norm"
    )

    train, test = data.randomSplit([0.8, 0.2], seed=42)
    model = pipeline.fit(train)
    preds = model.transform(test)

    evaluator = BinaryClassificationEvaluator(metricName="areaUnderROC")
    auc = evaluator.evaluate(preds)

    out_txt = reports_dir / "ml_results.txt"
    out_txt.write_text(
        "ML task: predict test_required (test_flag)\n"
        f"AUC (areaUnderROC): {auc}\n"
        "Features: salary_from, salary_to, salary_gross_bin, address_lat, area_id_num, salary_currency (OHE)\n",
        encoding="utf-8"
    )
    print(f"✅ ML report: {out_txt}  (AUC={auc:.4f})")


def main():
    ensure_winutils()

    if not STAGE_CSV.exists():
        raise FileNotFoundError(f"Не найден stage CSV: {STAGE_CSV}")

    spark = make_spark()
    print("✅ Spark:", spark.version)

    df_raw = (
        spark.read
        .option("header", True)
        .option("inferSchema", True)
        .csv(str(STAGE_CSV))
    )

    df = apply_salary_cleaning(df_raw)
    df = add_test_flag(df).cache()

    rows = df.count()
    print(f"📊 Rows: {rows}")

    # Аналитика 1: топ регионов (area_name)
    if "area_name" in df.columns:
        top_areas = (
            df.groupBy("area_name")
              .agg(F.count("*").alias("vacancies"))
              .orderBy(F.col("vacancies").desc())
              .limit(20)
        )
        out1 = REPORTS_DIR / "top_cities.csv"
        save_small_df_via_pandas(top_areas, out1, max_rows=20000)
        plot_bar_from_csv(out1, "area_name", "vacancies", REPORTS_DIR / "top_cities.png",
                          "Top-20 регионов по вакансиям")
    else:
        print("⚠️ Нет колонки area_name — пропускаю top_cities")


    if "area_name" in df.columns and "salary_from" in df.columns:
        salary_by_area = (
            df.filter(F.col("salary_from").isNotNull())
              .groupBy("area_name")
              .agg(
                  F.count("*").alias("with_salary"),
                  F.avg("salary_from").alias("avg_salary_from")
              )
              .filter(F.col("with_salary") >= 30)
              .orderBy(F.col("avg_salary_from").desc())
              .limit(30)
        )
        out2 = REPORTS_DIR / "salary_by_city.csv"
        save_small_df_via_pandas(salary_by_area, out2, max_rows=50000)
        plot_bar_from_csv(out2, "area_name", "avg_salary_from", REPORTS_DIR / "salary_by_city.png",
                          "Top-30 регионов по средней salary_from (min 30 вакансий)")
    else:
        print("⚠️ Не хватает area_name или salary_from — пропускаю salary_by_city")


    test_share = (
        df.agg(
            F.count("*").alias("total"),
            F.sum(F.col("test_flag").cast("long")).alias("with_test")
        )
        .withColumn("share", F.col("with_test") / F.col("total"))
    )
    out3 = REPORTS_DIR / "test_share.csv"
    save_small_df_via_pandas(test_share, out3, max_rows=10)


    train_simple_model(df, REPORTS_DIR)

    spark.stop()
    print("🛑 Spark остановлен")

if __name__ == "__main__":
    main()
