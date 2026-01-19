import sys
import traceback
from pathlib import Path
import subprocess


PROJECT_ROOT = Path(__file__).resolve().parent

# ⚠️ УКАЖИ ИМЯ stage-скрипта (который создаёт vacancies_stage.csv)
STAGE_SCRIPT = "etl_to_parquet.py"   # <-- поменяй на своё реальное имя


def run_step(title: str, script_name: str):
    """
    Запускает python-скрипт как отдельный процесс (надёжнее для Spark).
    """
    print("\n" + "=" * 70)
    print(f"▶ {title}")
    print("=" * 70)

    script_path = PROJECT_ROOT / script_name
    if not script_path.exists():
        raise FileNotFoundError(f"Не найден файл: {script_path}")

    # запускаем тем же python, который запустил run_pipeline.py
    cmd = [sys.executable, str(script_path)]
    print("CMD:", " ".join(cmd))

    result = subprocess.run(cmd, cwd=str(PROJECT_ROOT))

    if result.returncode != 0:
        raise RuntimeError(f"Шаг '{title}' завершился с ошибкой (код {result.returncode})")

    print(f"✅ {title} — OK")


def main():
    try:
        # 1) Stage
        run_step("STAGE: очистка + сохранение vacancies_stage.csv", STAGE_SCRIPT)

        # 2) Analytics + ML
        run_step("ANALYTICS + ML: отчёты + графики + ml_results.txt", "analytics_and_ml.py")

        # 3) Scalability benchmark
        run_step("SCALABILITY: эксперимент масштабируемости", "scalability_benchmark.py")

        print("\n" + "=" * 70)
        print("🎉 PIPELINE ГОТОВ: все шаги успешно выполнены")
        print("=" * 70)

        print("\n📌 Проверь результаты:")
        print(" - data/stage/vacancies_stage.csv")
        print(" - data/reports/top_cities.csv + top_cities.png")
        print(" - data/reports/salary_by_city.csv + salary_by_city.png")
        print(" - data/reports/test_share.csv")
        print(" - data/reports/ml_results.txt")
        print(" - data/reports/scalability_results.csv")
        print(" - data/reports/scalability_plot.png")

    except Exception as e:
        print("\n❌ PIPELINE УПАЛ С ОШИБКОЙ")
        print("Причина:", str(e))
        print("\nTRACEBACK:")
        traceback.print_exc()


if __name__ == "__main__":
    main()
