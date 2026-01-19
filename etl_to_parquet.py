import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

BASE_DIR = os.path.dirname(os.path.abspath(__file__))

INPUT_CSV = os.path.join(BASE_DIR, "IT_vacancies.csv")
OUT_DIR = os.path.join(BASE_DIR, "data", "stage")
OUT_FILE = os.path.join(OUT_DIR, "vacancies_stage.csv")


def to_double_safe(colname: str):
    s = F.col(colname)
    cleaned = F.regexp_replace(s, r"[^0-9,.\-]", "")
    cleaned = F.regexp_replace(cleaned, ",", ".")
    return F.when(cleaned.rlike(r"^-?\d+(\.\d+)?$"), cleaned.cast("double")).otherwise(F.lit(None).cast("double"))


if __name__ == "__main__":
    os.makedirs(OUT_DIR, exist_ok=True)

    spark = (
        SparkSession.builder
        .appName("vacancies_etl_stage_csv_python")
        .master("local[*]")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    print("✅ Spark:", spark.version)

    if not os.path.exists(INPUT_CSV):
        print("❌ Не найден файл:", INPUT_CSV)
        spark.stop()
        sys.exit(1)

    df_raw = (
        spark.read
        .option("header", True)
        .option("inferSchema", False)
        .option("multiLine", True)
        .option("escape", "\"")
        .option("quote", "\"")
        .csv(INPUT_CSV)
    )

    keep = [
        "id", "area_name", "name",
        "salary_from", "salary_to", "salary_currency",
        "experience", "employment", "schedule",
        "has_test", "premium", "archived",
        "count_key_skills", "published_at"
    ]
    keep = [c for c in keep if c in df_raw.columns]
    df = df_raw.select(*keep)

    for c in ["salary_from", "salary_to", "count_key_skills"]:
        if c in df.columns:
            df = df.withColumn(c, to_double_safe(c))

    for c in ["has_test", "premium", "archived"]:
        if c in df.columns:
            df = df.withColumn(c, F.when(F.lower(F.col(c)) == "true", 1).otherwise(0))

    if "id" in df.columns:
        df = df.dropDuplicates(["id"])


    try:
        import pandas as pd
    except ImportError:
        print("❌ Нет pandas. Установи: pip install pandas")
        spark.stop()
        sys.exit(1)

    pdf = df.toPandas()
    pdf.to_csv(OUT_FILE, index=False, encoding="utf-8-sig")

    print("✅ Stage CSV сохранён:", OUT_FILE)
    print("📊 Строк:", len(pdf))

    spark.stop()
    print("🛑 Spark остановлен")
