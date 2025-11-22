# import necessary libraries
import pandas as pd
import os
from pathlib import Path

# define root dir
PROJECT_ROOT = Path(__file__).resolve().parent

RAW_DATA_DIR = PROJECT_ROOT / "raw_data"
CLEANED_DATA_DIR = PROJECT_ROOT / "cleaned_data"

# import raw data
df = pd.read_csv('raw_data/flour4four_orders_oct2025.csv')
print('Data Loaded Successfully')

# handle missing records

df['order_date'] = df['order_date'].fillna(df['delivery_date'])

df['business_name'] = df['business_name'].fillna('Unknown')
df['business_type'] = df['business_type'].fillna('Unknown')
df['business_address'] = df['business_address'].fillna('Unknown')

df['flour_type'] = df['flour_type'].fillna('Unknown')

df['price_per_bag'] = df['price_per_bag'].fillna(df['price_per_bag'].median())

print("Missing Records Handled with Finesse!")

# converting data types

df['order_date'] = pd.to_datetime(df['order_date'])
df['delivery_date'] = pd.to_datetime(df['delivery_date'])

df['contact_phone'] = df['contact_phone'].astype(str)
df['rider_phone'] = df['rider_phone'].astype(str)

df['total_amount'] = df['total_amount'].astype(float)

print("Data Types handled Successfully")

df.to_csv(CLEANED_DATA_DIR / 'flour4four_orders_cleaned.csv', index=False)
print('Cleaned CSV created at cleaned_data/ ')