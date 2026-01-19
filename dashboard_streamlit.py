
from __future__ import annotations

import os
from pathlib import Path
import pandas as pd
import streamlit as st


HAVE_HIVE = True
try:
    from pyhive import hive
except Exception:
    HAVE_HIVE = False


PROJECT_DIR = Path(__file__).resolve().parent
REPORTS_DIR = PROJECT_DIR / "data" / "reports"
STAGE_DIR = PROJECT_DIR / "data" / "stage"

LOCAL_TOP_CITIES = REPORTS_DIR / "top_cities.csv"
LOCAL_SALARY_BY_CITY = REPORTS_DIR / "salary_by_city.csv"
LOCAL_TEST_SHARE = REPORTS_DIR / "test_share.csv"
LOCAL_SCALABILITY = REPORTS_DIR / "scalability_results.csv"


DEFAULT_HIVE_HOST = os.environ.get("HIVE_HOST", "localhost")
DEFAULT_HIVE_PORT = int(os.environ.get("HIVE_PORT", "10000"))
DEFAULT_HIVE_USER = os.environ.get("HIVE_USER", "root")
DEFAULT_HIVE_DB = os.environ.get("HIVE_DB", "samsung")


@st.cache_data(ttl=60, show_spinner=False)
def hive_query(sql: str, host: str, port: int, user: str, database: str) -> pd.DataFrame:
    if not HAVE_HIVE:
        raise RuntimeError("PyHive не установлен. Установи: pip install pyhive thrift thrift-sasl")

    conn = hive.Connection(host=host, port=port, username=user, database=database)

    df = pd.read_sql(sql, conn)
    conn.close()
    return df


@st.cache_data(ttl=10, show_spinner=False)
def read_csv_safe(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Не найден файл: {path}")

    return pd.read_csv(path, encoding="utf-8")

def show_local_artifacts():
    st.subheader("📦 Local (PySpark) — готовые результаты из data/reports")


    if (REPORTS_DIR / "top_cities.csv").exists():
        st.markdown("### 🏙️ Топ городов")
        st.dataframe(pd.read_csv(REPORTS_DIR / "top_cities.csv", encoding="utf-8"), use_container_width=True)
    if (REPORTS_DIR / "top_cities.png").exists():
        st.image(str(REPORTS_DIR / "top_cities.png"), use_container_width=True)

    st.divider()


    if (REPORTS_DIR / "salary_by_city.csv").exists():
        st.markdown("### 💰 Зарплата по городам")
        st.dataframe(pd.read_csv(REPORTS_DIR / "salary_by_city.csv", encoding="utf-8"), use_container_width=True)
    if (REPORTS_DIR / "salary_by_city.png").exists():
        st.image(str(REPORTS_DIR / "salary_by_city.png"), use_container_width=True)

    st.divider()


    if (REPORTS_DIR / "test_share.csv").exists():
        st.markdown("### 🧪 Доля вакансий с тестом")
        st.dataframe(pd.read_csv(REPORTS_DIR / "test_share.csv", encoding="utf-8"), use_container_width=True)

    st.divider()


    ml_path = REPORTS_DIR / "ml_results.txt"
    if ml_path.exists():
        st.markdown("### 🤖 ML результаты")
        st.code(ml_path.read_text(encoding="utf-8", errors="replace"))

    st.divider()

    # 5) Scalability
    if (REPORTS_DIR / "scalability_results.csv").exists():
        st.markdown("### 📈 Масштабируемость (таблица)")
        st.dataframe(pd.read_csv(REPORTS_DIR / "scalability_results.csv", encoding="utf-8"), use_container_width=True)

    if (REPORTS_DIR / "scalability_plot.png").exists():
        st.markdown("### 📈 Масштабируемость (график)")
        st.image(str(REPORTS_DIR / "scalability_plot.png"), use_container_width=True)


def header():
    st.set_page_config(page_title="IT Vacancies Dashboard", layout="wide")
    st.title("📊 IT Vacancies — Dashboard")
    st.caption("Источник данных переключается: Hive (Docker) или локальные CSV (PyCharm).")


def sidebar_source_picker():
    st.sidebar.header("⚙️ Настройки")

    source = st.sidebar.radio(
        "Источник данных",
        ["Hive (Docker)", "Local CSV (PyCharm)"],
        index=0,
    )

    hive_cfg = None
    if source == "Hive (Docker)":
        st.sidebar.subheader("Hive подключение")
        host = st.sidebar.text_input("Host", value=DEFAULT_HIVE_HOST)
        port = st.sidebar.number_input("Port", value=DEFAULT_HIVE_PORT, step=1)
        user = st.sidebar.text_input("User", value=DEFAULT_HIVE_USER)
        database = st.sidebar.text_input("Database", value=DEFAULT_HIVE_DB)

        hive_cfg = {"host": host, "port": int(port), "user": user, "database": database}

        if not HAVE_HIVE:
            st.sidebar.error("Нет PyHive. Установи: pip install pyhive thrift thrift-sasl")

    st.sidebar.divider()
    st.sidebar.write("📁 Local reports dir:")
    st.sidebar.code(str(REPORTS_DIR))

    return source, hive_cfg


def load_top_cities(source: str, hive_cfg: dict | None) -> pd.DataFrame:
    """
    Ожидаем top_cities.csv как минимум колонку city/area_name + count.
    В Hive будем брать из it_vacancies_clean или it_salary_by_area.
    """
    if source == "Local CSV (PyCharm)":
        df = read_csv_safe(LOCAL_TOP_CITIES)
        return df

    # Hive mode
    assert hive_cfg is not None
    # Топ городов по числу вакансий (по всей базе)
    sql = """
    SELECT
      area_name AS city,
      COUNT(*)  AS vacancies
    FROM it_vacancies_clean
    GROUP BY area_name
    ORDER BY vacancies DESC
    LIMIT 30
    """
    return hive_query(sql, **hive_cfg)


def load_salary_by_city(source: str, hive_cfg: dict | None) -> pd.DataFrame:
    """
    salary_by_city.csv у тебя уже есть локально.
    В Hive — берем it_salary_by_area (avg_rub) и vacancies_with_salary.
    """
    if source == "Local CSV (PyCharm)":
        df = read_csv_safe(LOCAL_SALARY_BY_CITY)
        return df

    assert hive_cfg is not None
    sql = """
    SELECT
      area_name AS city,
      vacancies_with_salary,
      avg_rub
    FROM it_salary_by_area
    WHERE avg_rub IS NOT NULL
    ORDER BY vacancies_with_salary DESC
    LIMIT 30
    """
    return hive_query(sql, **hive_cfg)


def load_test_share(source: str, hive_cfg: dict | None) -> pd.DataFrame:
    """
    test_share.csv локально есть.
    В Hive: в raw/clean есть has_test или test_required (в raw точно есть test_required как string).
    В clean у тебя нет поля has_test, поэтому считаем по raw: test_required='True'
    """
    if source == "Local CSV (PyCharm)":
        return read_csv_safe(LOCAL_TEST_SHARE)

    assert hive_cfg is not None
    sql = """
    SELECT
      CASE WHEN lower(test_required)='true' THEN 'test_required=true' ELSE 'test_required=false/empty' END AS test_flag,
      COUNT(*) AS cnt
    FROM it_vacancies_raw
    GROUP BY CASE WHEN lower(test_required)='true' THEN 'test_required=true' ELSE 'test_required=false/empty' END
    """
    return hive_query(sql, **hive_cfg)


def load_scalability(source: str) -> pd.DataFrame | None:
    """
    Scalability у тебя считается локально в PySpark и сохраняется как CSV.
    В Hive его обычно не кладут — поэтому показываем только из Local.
    """
    if source == "Local CSV (PyCharm)":
        try:
            return read_csv_safe(LOCAL_SCALABILITY)
        except Exception:
            return None
    return None


def main():
    header()
    source, hive_cfg = sidebar_source_picker()


    show_artifacts = False
    if source == "Local CSV (PyCharm)":
        show_artifacts = st.sidebar.checkbox(
            "Показывать сохранённые артефакты (PNG/CSV)",
            value=True,
        )


    c1, c2, c3 = st.columns(3)
    with c1:
        st.metric("Источник", source)
    with c2:
        st.metric("Reports dir exists", "YES" if REPORTS_DIR.exists() else "NO")
    with c3:
        st.metric("Hive driver", "OK" if (source != "Hive (Docker)" or HAVE_HIVE) else "MISSING")


    if source == "Local CSV (PyCharm)" and show_artifacts:
        st.divider()
        with st.expander("📦 Local (PySpark) — сохранённые результаты из data/reports (как в пайплайне)", expanded=True):
            show_local_artifacts()

    st.divider()


    st.subheader("🏙️ Топ городов по числу вакансий")
    try:
        df_top = load_top_cities(source, hive_cfg)
        st.dataframe(df_top, use_container_width=True)

        # normalize columns for chart
        cols = [c.lower() for c in df_top.columns]
        if "vacancies" in cols:
            city_col = df_top.columns[cols.index("city")] if "city" in cols else df_top.columns[0]
            val_col = df_top.columns[cols.index("vacancies")]
        else:
            city_col = df_top.columns[0]
            val_col = df_top.columns[1] if len(df_top.columns) > 1 else df_top.columns[0]

        chart_df = df_top[[city_col, val_col]].copy()
        chart_df.columns = ["city", "vacancies"]
        chart_df["vacancies"] = pd.to_numeric(chart_df["vacancies"], errors="coerce")
        chart_df = chart_df.dropna(subset=["vacancies"]).sort_values("vacancies", ascending=True).tail(20)
        st.bar_chart(chart_df.set_index("city"), height=420)


        if source == "Local CSV (PyCharm)":
            png = REPORTS_DIR / "top_cities.png"
            if png.exists():
                st.caption("PNG из пайплайна:")
                st.image(str(png), use_container_width=True)

    except Exception as e:
        st.error(f"Не удалось загрузить top cities: {e}")

    st.divider()


    st.subheader("💰 Зарплата по городам (RUB) + кол-во вакансий с зарплатой")
    try:
        df_sal = load_salary_by_city(source, hive_cfg)
        st.dataframe(df_sal, use_container_width=True)

        cols = [c.lower() for c in df_sal.columns]
        city = df_sal.columns[cols.index("city")] if "city" in cols else df_sal.columns[0]
        avg = df_sal.columns[cols.index("avg_rub")] if "avg_rub" in cols else df_sal.columns[-1]

        chart_df = df_sal[[city, avg]].copy()
        chart_df.columns = ["city", "avg_rub"]
        chart_df["avg_rub"] = pd.to_numeric(chart_df["avg_rub"], errors="coerce")
        chart_df = chart_df.dropna(subset=["avg_rub"]).sort_values("avg_rub", ascending=True).tail(20)
        st.bar_chart(chart_df.set_index("city"), height=420)

        if source == "Local CSV (PyCharm)":
            png = REPORTS_DIR / "salary_by_city.png"
            if png.exists():
                st.caption("PNG из пайплайна:")
                st.image(str(png), use_container_width=True)

    except Exception as e:
        st.error(f"Не удалось загрузить salary by city: {e}")

    st.divider()

    # ---- TEST SHARE ----
    st.subheader("🧪 Доля вакансий с тестовым заданием")
    try:
        df_test = load_test_share(source, hive_cfg)
        st.dataframe(df_test, use_container_width=True)

        if len(df_test.columns) >= 2:
            label_col = df_test.columns[0]
            cnt_col = df_test.columns[1]
            plot_df = df_test[[label_col, cnt_col]].copy()
            plot_df.columns = ["flag", "cnt"]
            plot_df["cnt"] = pd.to_numeric(plot_df["cnt"], errors="coerce").fillna(0)
            st.bar_chart(plot_df.set_index("flag"), height=260)

    except Exception as e:
        st.error(f"Не удалось загрузить test share: {e}")

    st.divider()


    st.subheader("🤖 ML результаты")
    if source == "Local CSV (PyCharm)":
        ml_path = REPORTS_DIR / "ml_results.txt"
        if ml_path.exists():
            st.code(ml_path.read_text(encoding="utf-8", errors="replace"))
        else:
            st.info("Файл ml_results.txt не найден. Запусти analytics_and_ml.py.")
    else:
        st.info("")

    st.divider()


    st.subheader("📈 Эксперимент масштабируемости")
    df_sc = load_scalability(source)
    if df_sc is None:
        st.info("")
    else:
        st.dataframe(df_sc, use_container_width=True)

        lc = [c.lower() for c in df_sc.columns]
        if "m" in lc:
            mcol = df_sc.columns[lc.index("m")]
            tcol = None
            for cand in ["avg", "avg_sec", "avg_seconds", "time", "avg_time"]:
                if cand in lc:
                    tcol = df_sc.columns[lc.index(cand)]
                    break
            if tcol is None and len(df_sc.columns) >= 2:
                tcol = df_sc.columns[1]

            if tcol is not None:
                plot_df = df_sc[[mcol, tcol]].copy()
                plot_df.columns = ["multiplier_m", "time"]
                plot_df["time"] = pd.to_numeric(plot_df["time"], errors="coerce")
                plot_df = plot_df.dropna(subset=["time"]).sort_values("multiplier_m")
                st.line_chart(plot_df.set_index("multiplier_m"), height=300)

        if source == "Local CSV (PyCharm)":
            png = REPORTS_DIR / "scalability_plot.png"
            if png.exists():
                st.caption("PNG из пайплайна:")
                st.image(str(png), use_container_width=True)

    st.caption("✅ ")



if __name__ == "__main__":
    main()
