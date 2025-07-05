import os
import pandas as pd
from clickhouse_connect import get_client
from dotenv import load_dotenv
load_dotenv()

def get_last10_arenda_chiko():
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
            "День",
            "Подразделение",
            "Аренда"
        FROM "20-01-03-1"
        WHERE "Подразделение" = 'ЧИКО'
        ORDER BY "День" DESC
        LIMIT 10
    """
    result = client.query(query)
    df = pd.DataFrame(result.result_rows, columns=result.column_names)
    if 'День' in df.columns:
        df['День'] = pd.to_datetime(df['День'], errors='coerce')
    return df

if __name__ == "__main__":
    df = get_last10_arenda_chiko()
    print(df)
    print(f"Сумма аренды по 10 записям: {df['Аренда'].sum():,.2f}₽")
