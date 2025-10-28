#Imports and MySQL connection setup

import pandas as pd
import requests
import mysql.connector
from sqlalchemy import create_engine, text
import streamlit as st
import matplotlib.pyplot as plt
import seaborn as sns

# Replace with your MySQL credentials
MYSQL_USER = "root"
MYSQL_PASSWORD = "12345677"
MYSQL_HOST = "localhost"
MYSQL_PORT = 3306
MYSQL_DATABASE = "chocodb"

def get_engine():
    return create_engine(f"mysql+mysqlconnector://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DATABASE}", echo=False)

#API data extraction from OpenFoodFacts

def fetch_chocolate_data(pages=120):
    base_url = "https://world.openfoodfacts.org/api/v2/search?categories=chocolates&fields=code,product_name,brands,nutriments&page_size=100&page="
    all_data = []
    for page in range(1, pages+1):
        url = base_url + str(page)
        response = requests.get(url)
        if response.status_code != 200:
            break
        page_data = response.json().get('products', [])
        all_data.extend(page_data)
    return pd.json_normalize(all_data)

@st.cache_data(show_spinner=True)
def load_data():
    df = fetch_chocolate_data()
    return df

#Data cleaning and feature engineering
def clean_and_feature_eng(df):
    thresh = len(df) * 0.3
    df = df.dropna(axis=1, thresh=thresh)

    nut_cols = {
        'nutriments.energy-kcal_value': 'energy_kcal',
        'nutriments.energy_kj_value': 'energy_kj',
        'nutriments.sugars_value': 'sugars',
        'nutriments.carbohydrates_value': 'carbohydrates',
        'nutriments.fat_value': 'fat',
        'nutriments.saturated-fat_value': 'saturated_fat',
        'nutriments.proteins_value': 'proteins',
        'nutriments.fiber_value': 'fiber',
        'nutriments.sodium_value': 'sodium'
    }

    for old, new in nut_cols.items():
        if old in df.columns:
            df.rename(columns={old: new}, inplace=True)
            df[new].fillna(0, inplace=True)

    df.rename(columns={
        'code': 'product_code',
        'product_name': 'product_name',
        'brands': 'brand'
    }, inplace=True)

    if 'sugars' in df.columns and 'carbohydrates' in df.columns:
        df['sugar_to_carb_ratio'] = df.apply(lambda r: r['sugars']/r['carbohydrates'] if r['carbohydrates'] > 0 else 0, axis=1)
    else:
        df['sugar_to_carb_ratio'] = 0

    if 'energy_kcal' in df.columns:
        df['calorie_category'] = pd.cut(df['energy_kcal'], bins=[-1,100,250,10000], labels=['Low','Moderate','High'])
    else:
        df['calorie_category'] = 'Low'

    if 'sugars' in df.columns:
        df['sugar_category'] = pd.cut(df['sugars'], bins=[-1,5,15,10000], labels=['Low Sugar','Moderate Sugar','High Sugar'])
    else:
        df['sugar_category'] = 'Low Sugar'

    if 'nova_group' in df.columns:
        df['is_ultra_processed'] = df['nova_group'].apply(lambda x: 'Yes' if x == 4 else 'No')
    else:
        df['is_ultra_processed'] = 'No'

    return df

#MySQL schema creation
def create_mysql_schema():
    engine = get_engine()
    with engine.begin() as conn:
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS product_info (
            product_code VARCHAR(255) PRIMARY KEY,
            product_name VARCHAR(1024),
            brand VARCHAR(255)
        )"""))
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS nutrient_info (
            product_code VARCHAR(255),
            energy_kcal FLOAT,
            sugars FLOAT,
            carbohydrates FLOAT,
            nova_group INT,
            FOREIGN KEY (product_code) REFERENCES product_info(product_code)
        )"""))
        conn.execute(text("""
        CREATE TABLE IF NOT EXISTS derived_metrics (
            product_code VARCHAR(255),
            sugar_to_carb_ratio FLOAT,
            calorie_category VARCHAR(50),
            sugar_category VARCHAR(50),
            is_ultra_processed VARCHAR(5),
            FOREIGN KEY (product_code) REFERENCES product_info(product_code)
        )"""))
    return engine

#Data insertion into MySQL
def insert_into_mysql(df, engine=None):
    if engine is None:
        engine = get_engine()
    to_product = df[['product_code','product_name','brand']].drop_duplicates()
    to_nutrient = df[['product_code','energy_kcal','sugars','carbohydrates','nova_group']].drop_duplicates()
    to_derived = df[['product_code','sugar_to_carb_ratio','calorie_category','sugar_category','is_ultra_processed']].drop_duplicates()

    with engine.begin() as conn:
        to_product.to_sql('product_info', con=conn, if_exists='append', index=False)
        to_nutrient.to_sql('nutrient_info', con=conn, if_exists='append', index=False)
        to_derived.to_sql('derived_metrics', con=conn, if_exists='append', index=False)

#SQL queries
queries = {
    "Count products per brand": "SELECT brand, COUNT(*) AS product_count FROM product_info GROUP BY brand;",
    "Count unique products per brand": "SELECT brand, COUNT(DISTINCT product_name) AS unique_products FROM product_info GROUP BY brand;",
    "Top 5 brands by product count": "SELECT brand, COUNT(*) AS product_count FROM product_info GROUP BY brand ORDER BY product_count DESC LIMIT 5;",
    "Products with missing product name": "SELECT * FROM product_info WHERE product_name IS NULL OR product_name = '';",
    "Number of unique brands": "SELECT COUNT(DISTINCT brand) FROM product_info;",
    "Products with code starting with '3'": "SELECT * FROM product_info WHERE product_code LIKE '3%';",

    "Top 10 products with highest energy_kcal": "SELECT product_code, energy_kcal FROM nutrient_info ORDER BY energy_kcal DESC LIMIT 10;",
    "Average sugars per nova group": "SELECT nova_group, AVG(sugars) AS avg_sugars FROM nutrient_info GROUP BY nova_group;",
    "Count products with fat_value > 20": "SELECT COUNT(*) FROM nutrient_info WHERE fat_value > 20;",
    "Average carbohydrates per product": "SELECT AVG(carbohydrates) FROM nutrient_info;",
    "Products with sodium_value > 1": "SELECT * FROM nutrient_info WHERE sodium_value > 1;",
    "Non-zero fruits/vegetables/nuts content": "SELECT COUNT(*) FROM nutrient_info WHERE fruits_vegetables_nuts_estimate_from_ingredients_100g > 0;",
    "Energy kcal > 500": "SELECT * FROM nutrient_info WHERE energy_kcal > 500;",

    "Count per calorie category": "SELECT calorie_category, COUNT(*) FROM derived_metrics GROUP BY calorie_category;",
    "Count High Sugar": "SELECT COUNT(*) FROM derived_metrics WHERE sugar_category = 'High Sugar';",
    "Avg sugar_to_carb for High calories": "SELECT AVG(sugar_to_carb_ratio) FROM derived_metrics WHERE calorie_category = 'High';",
    "High Calorie and High Sugar": "SELECT * FROM derived_metrics WHERE calorie_category = 'High' AND sugar_category = 'High Sugar';",
    "Ultra-processed count": "SELECT COUNT(*) FROM derived_metrics WHERE is_ultra_processed = 'Yes';",
    "sugar_to_carb_ratio > 0.7": "SELECT * FROM derived_metrics WHERE sugar_to_carb_ratio > 0.7;",
    "Avg sugar_to_carb by calorie": "SELECT calorie_category, AVG(sugar_to_carb_ratio) FROM derived_metrics GROUP BY calorie_category;",

    "Top brands with most High Calorie products": """
        SELECT p.brand, COUNT(*) AS high_calories_count
        FROM product_info p
        JOIN derived_metrics d ON p.product_code = d.product_code
        WHERE d.calorie_category = 'High'
        GROUP BY p.brand
        ORDER BY high_calories_count DESC
        LIMIT 5;
    """,
    "Average energy by calorie category": """
        SELECT d.calorie_category, AVG(n.energy_kcal)
        FROM derived_metrics d
        JOIN nutrient_info n ON d.product_code = n.product_code
        GROUP BY d.calorie_category;
    """,
    "Ultra-processed per brand": """
        SELECT p.brand, COUNT(*) AS ultra_processed_count
        FROM product_info p
        JOIN derived_metrics d ON p.product_code = d.product_code
        WHERE d.is_ultra_processed = 'Yes'
        GROUP BY p.brand;
    """,
    "High Sugar and High Calorie with brand": """
        SELECT p.product_name, p.brand, d.calorie_category, d.sugar_category
        FROM product_info p
        JOIN derived_metrics d ON p.product_code = d.product_code
        WHERE d.calorie_category = 'High' AND d.sugar_category = 'High Sugar';
    """,
    "Avg sugars by brand for ultra-processed": """
        SELECT p.brand, AVG(n.sugars) AS avg_sugars
        FROM product_info p
        JOIN derived_metrics d ON p.product_code = d.product_code
        JOIN nutrient_info n ON p.product_code = n.product_code
        WHERE d.is_ultra_processed = 'Yes'
        GROUP BY p.brand;
    """,
    "Fruits/Nuts content by calorie": """
        SELECT d.calorie_category, COUNT(*)
        FROM derived_metrics d
        JOIN nutrient_info n ON d.product_code = n.product_code
        WHERE n.fruits_vegetables_nuts_estimate_from_ingredients_100g > 0
        GROUP BY d.calorie_category;
    """,
    "Top 5 products by sugar_to_carb_ratio": """
        SELECT p.product_name, d.sugar_to_carb_ratio, d.calorie_category, d.sugar_category
        FROM product_info p
        JOIN derived_metrics d ON p.product_code = d.product_code
        ORDER BY d.sugar_to_carb_ratio DESC
        LIMIT 5;
    """
}

#Streamlit interactive UI code
def main():
    st.title("ChocoCrunch Analytics — MySQL backend")
    df = load_data()
    cleaned_df = clean_and_feature_eng(df)

    if st.sidebar.button("Create MySQL Schema & Insert Data"):
        engine = get_engine()
        create_mysql_schema()
        insert_into_mysql(cleaned_df, engine)
        st.success("MySQL schema created and data inserted.")

    if st.sidebar.checkbox("Show Raw Data"):
        st.dataframe(df.head())

    if st.sidebar.checkbox("Show Cleaned Data"):
        st.dataframe(cleaned_df.head())

    query_choice = st.sidebar.selectbox("Run SQL Query", list(queries.keys()))
    if st.sidebar.button("Execute Query"):
        engine = get_engine()
        with engine.connect() as conn:
            result = conn.execute(text(queries[query_choice]))
            result_df = pd.DataFrame(result.fetchall(), columns=result.keys())
            st.dataframe(result_df)

    if st.sidebar.button("Show Calorie Category Distribution"):
        plt.figure(figsize=(10,6))
        sns.countplot(x=cleaned_df['calorie_category'].astype(str))
        plt.title("Calorie Category Distribution")
        st.pyplot(plt.gcf())
        plt.clf()

if __name__ == "__main__":
    main()

