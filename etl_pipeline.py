import pandas as pd
import sqlite3
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def extract_raw_data():
    raw_path = Path("data/raw/superstore.csv")
    logger.info(f"Читаю {raw_path}")
    df = pd.read_csv(raw_path)
    logger.info(f"Загружено {len(df)} строк")
    return df

def transform_data(df):
    logger.info("Начинаю трансформацию...")

    # приводим дату к формату datetime
    df["Order Date"] = pd.to_datetime(df["Order Date"], dayfirst=True, errors="coerce")  
    df = df.dropna(subset=["Order Date"])  

    # удаляем строки без ключевых полей
    df = df.dropna(subset=["Order ID", "Sales"])

    # новые поля
    df["Year"] = df["Order Date"].dt.year
    df["Month"] = df["Order Date"].dt.month
    df["Quarter"] = df["Order Date"].dt.quarter

    # выручка по заказу
    df["Revenue_per_order"] = df.groupby("Order ID")["Sales"].transform("sum")

    logger.info(f"Трансформация завершена, строк: {len(df)}")
    return df

def load_to_db(df):
    db_path = Path("data/processed/superstore.db")
    conn = sqlite3.connect(db_path)
    df.to_sql("sales", conn, if_exists="replace", index=False)
    conn.close()
    logger.info(f"Данные сохранены в {db_path}")

if __name__ == "__main__":
    df_raw = extract_raw_data()
    df_clean = transform_data(df_raw)
    load_to_db(df_clean)
    print("ETL PIPELINE выполнен успешно")  

def run_etl():
    """Запуск полного ETL-пайплайна (для Airflow)."""
    df_raw = extract_raw_data()
    df_clean = transform_data(df_raw)
    load_to_db(df_clean)