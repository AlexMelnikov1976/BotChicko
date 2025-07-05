import pandas as pd
from datetime import datetime
from clickhouse_connect import get_client
import os

def get_taxi_data():
    client = get_client(
        host=os.getenv('CLICKHOUSE_HOST'),
        port=int(os.getenv('CLICKHOUSE_PORT')),
        username=os.getenv('CLICKHOUSE_USER'),
        password=os.getenv('CLICKHOUSE_PASSWORD'),
        database=os.getenv('CLICKHOUSE_DB'),
        secure=os.getenv('CLICKHOUSE_SECURE') == 'True',
        verify=os.getenv('CLICKHOUSE_CA')
    )
    query = """
        SELECT
            "ДатаЗаказа",
            "Подразделение",
            "СтоимостьСНДС",
            "КолВоПоездок"
        FROM "Taxi"
        LIMIT 1000
    """
    result = client.query(query)
    df = pd.DataFrame(result.result_rows, columns=result.column_names)
    if 'ДатаЗаказа' in df.columns:
        df['ДатаЗаказа'] = pd.to_datetime(df['ДатаЗаказа'], errors='coerce')
    df = df[df['Подразделение'] == "Чико"]
    return df

def get_taxi_summary_for_date(target_date):
    try:
        df_taxi = get_taxi_data()
        if df_taxi.empty:
            return "нет данных", 0, 0
        df_taxi["ДатаЗаказа"] = pd.to_datetime(df_taxi["ДатаЗаказа"]).dt.date
        day_df = df_taxi[df_taxi["ДатаЗаказа"] == target_date.date()]
        if day_df.empty:
            return "нет данных", 0, 0
        total_sum = day_df["СтоимостьСНДС"].sum()
        total_rides = day_df["КолВоПоездок"].sum()
        return None, total_sum, total_rides
    except Exception as e:
        return str(e), 0, 0
