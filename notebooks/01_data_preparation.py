"""
Data Preparation Script for Streaming Platform Churn Analysis
Handles: Dataset download, cleaning, and transformation
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

# ============================================================================
# STEP 1: Download and Load Dataset
# ============================================================================
print("=" * 80)
print("STEP 1: Downloading Telco Customer Churn Dataset")
print("=" * 80)

print("Checking for dataset...")


# ============================================================================
# STEP 2: Load and Explore Data
# ============================================================================
print("\n" + "=" * 80)
print("STEP 2: Loading and Exploring Data")
print("=" * 80)

df = pd.read_csv('../data/WA_Fn-UseC_-Telco-Customer-Churn.csv')

print(f"\nDataset Shape: {df.shape}")
df = pd.read_csv('../data/WA_Fn-UseC_-Telco-Customer-Churn.csv')

print(f"\nDataset Shape: {df.shape}")
print(f"Columns: {df.columns.tolist()}")
print(f"Data Types:\n{df.dtypes}")
print(f"Missing Values:\n{df.isnull().sum()}")


# ============================================================================
# STEP 3: Data Transformation
# ============================================================================
print("\n" + "=" * 80)
print("STEP 3: Transforming Data")
print("=" * 80)

# Create a copy for transformation
df_clean = df.copy()

# Fix data types
df_clean['TotalCharges'] = pd.to_numeric(df_clean['TotalCharges'], errors='coerce')

# Generate synthetic dates (production would use real churn dates)
print("Generating temporal data...")
base_date = datetime(2020, 1, 1)
df_clean['acquisition_date'] = [base_date + timedelta(days=int(np.random.uniform(0, 730)))
                                 for _ in range(len(df_clean))]

# If churned, add churn date after acquisition
df_clean['churn_date'] = df_clean.apply(
    lambda row: (row['acquisition_date'] + timedelta(days=int(np.random.uniform(30, 365))))
    if row['Churn'] == 'Yes' else pd.NaT,
    axis=1
)

# Map subscription tiers based on internet service and tenure
def assign_tier(row):
    if row['InternetService'] == 'No':
        return 'Basic'
    elif row['tenure'] > 12:
        return 'Premium'
    else:
        return 'Basic'

df_clean['subscription_tier'] = df_clean.apply(assign_tier, axis=1)

# Assign acquisition channels
channels = ['Direct Sales', 'Online Marketing', 'Partner', 'Referral', 'Phone']
df_clean['acquisition_channel'] = np.random.choice(channels, len(df_clean))

# Rename columns to match schema
column_mapping = {
    'customerID': 'customer_id',
    'gender': 'gender',
    'SeniorCitizen': 'senior_citizen',
    'Partner': 'partner',
    'Dependents': 'dependents',
    'tenure': 'tenure_months',
    'PhoneService': 'phone_service',
    'MultipleLines': 'multiple_lines',
    'InternetService': 'internet_service',
    'OnlineSecurity': 'online_security',
    'OnlineBackup': 'online_backup',
    'DeviceProtection': 'device_protection',
    'TechSupport': 'tech_support',
    'StreamingTV': 'streaming_tv',
    'StreamingMovies': 'streaming_movies',
    'Contract': 'contract',
    'PaperlessBilling': 'paperless_billing',
    'PaymentMethod': 'payment_method',
    'MonthlyCharges': 'monthly_charges',
    'TotalCharges': 'total_charges',
    'Churn': 'churn'
}

df_clean = df_clean.rename(columns=column_mapping)
# Keep renamed columns + the new columns we added
keep_columns = list(column_mapping.values()) + ['subscription_tier', 'acquisition_channel', 'churn_date', 'acquisition_date']
df_clean = df_clean[keep_columns]

print("✓ Data transformed successfully")
print(f"Final dataset shape: {df_clean.shape}")


# ============================================================================
# STEP 4: Data Validation
# ============================================================================
print("\n" + "=" * 80)
print("STEP 4: Data Validation")
print("=" * 80)

print(f"\nChurn Distribution:")
print(df_clean['churn'].value_counts())

print(f"\nSubscription Tier Distribution:")
print(df_clean['subscription_tier'].value_counts())

print(f"\nAcquisition Channel Distribution:")
print(df_clean['acquisition_channel'].value_counts())

print(f"\nTenure Statistics:")
print(df_clean['tenure_months'].describe())


# ============================================================================
# STEP 5: Export for Database Import
# ============================================================================
print("\n" + "=" * 80)
print("STEP 5: Exporting Data")
print("=" * 80)

# Export to CSV for database import (in correct database column order)
csv_path = '../data/telco_churn_clean.csv'
column_order = [
    'customer_id', 'gender', 'senior_citizen', 'partner', 'dependents', 'tenure_months',
    'phone_service', 'multiple_lines', 'internet_service', 'online_security', 'online_backup',
    'device_protection', 'tech_support', 'streaming_tv', 'streaming_movies', 'contract',
    'paperless_billing', 'payment_method', 'monthly_charges', 'total_charges', 'churn',
    'churn_date', 'acquisition_date', 'acquisition_channel', 'subscription_tier'
]
# Keep only columns that exist
column_order = [col for col in column_order if col in df_clean.columns]
df_clean[column_order].to_csv(csv_path, index=False)
print(f"✓ Exported to: {csv_path}")

# Export summary statistics
summary_stats = {
    'total_records': len(df_clean),
    'churn_rate': (df_clean['churn'] == 'Yes').sum() / len(df_clean) * 100,
    'avg_tenure': df_clean['tenure_months'].mean(),
    'avg_monthly_charges': df_clean['monthly_charges'].mean(),
    'total_revenue': df_clean['total_charges'].sum()
}

print("\nDataset Summary:")
for key, value in summary_stats.items():
    if isinstance(value, (int, float)):
        print(f"  {key}: {value:.2f}" if isinstance(value, float) else f"  {key}: {value}")


# ============================================================================
# STEP 6: Generate SQL INSERT Statements
# ============================================================================
print("\n" + "=" * 80)
print("STEP 6: Generating SQL Import Script")
print("=" * 80)

# PostgreSQL COPY command (fastest for bulk insert)
copy_command = f"""
COPY customers (
    customer_id, gender, senior_citizen, partner, dependents, tenure_months,
    phone_service, multiple_lines, internet_service, online_security, online_backup,
    device_protection, tech_support, streaming_tv, streaming_movies, contract,
    paperless_billing, payment_method, monthly_charges, total_charges, churn,
    churn_date, acquisition_date, acquisition_channel, subscription_tier
) FROM STDIN WITH (FORMAT csv, HEADER true, NULL 'NULL');
"""

sql_path = '../sql/03_import_data.sql'
with open(sql_path, 'w') as f:
    f.write("-- Data Import Script\n")
    f.write("-- Use with: psql -d database_name -f 03_import_data.sql\n\n")
    f.write(copy_command)

print(f"✓ Generated import script: {sql_path}")

print("\n" + "=" * 80)
print("✓ DATA PREPARATION COMPLETE")
print("=" * 80)
print("\nNext Steps:")
print("1. Set up database (PostgreSQL recommended)")
print("2. Run: psql -d your_db -f sql/00_schema.sql")
print("3. Import data: psql -d your_db -c \"COPY ... FROM 'data/telco_churn_clean.csv' WITH (FORMAT csv, HEADER)\"")
print("4. Run analysis queries: psql -d your_db -f sql/01_churn_analysis.sql")
