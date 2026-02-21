-- Database Schema for Streaming Platform Churn Analysis
-- Compatible with PostgreSQL, MySQL, SQLite

-- Main Customers Table
CREATE TABLE customers (
    customer_id VARCHAR(50) PRIMARY KEY,
    gender VARCHAR(20),
    senior_citizen INT,
    partner VARCHAR(10),
    dependents VARCHAR(10),
    tenure_months INT,
    phone_service VARCHAR(10),
    multiple_lines VARCHAR(20),
    internet_service VARCHAR(20),
    online_security VARCHAR(20),
    online_backup VARCHAR(20),
    device_protection VARCHAR(20),
    tech_support VARCHAR(20),
    streaming_tv VARCHAR(20),
    streaming_movies VARCHAR(20),
    contract VARCHAR(20),
    paperless_billing VARCHAR(10),
    payment_method VARCHAR(50),
    monthly_charges DECIMAL(10,2),
    total_charges DECIMAL(10,2),
    churn VARCHAR(10),
    churn_date DATE,
    acquisition_date DATE,
    acquisition_channel VARCHAR(50),
    subscription_tier VARCHAR(20)
);

-- Customer Events Table (for time-series analysis)
CREATE TABLE customer_events (
    event_id SERIAL PRIMARY KEY,
    customer_id VARCHAR(50),
    event_date DATE,
    event_type VARCHAR(50),
    subscription_tier VARCHAR(20),
    monthly_charges DECIMAL(10,2),
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

-- Monthly Metrics Snapshot
CREATE TABLE monthly_metrics (
    metric_date DATE,
    subscription_tier VARCHAR(20),
    total_active_subscribers INT,
    new_subscribers INT,
    churned_subscribers INT,
    total_revenue DECIMAL(15,2),
    avg_monthly_charges DECIMAL(10,2),
    PRIMARY KEY (metric_date, subscription_tier)
);

CREATE INDEX idx_customers_churn ON customers(churn);
CREATE INDEX idx_customers_acquisition_date ON customers(acquisition_date);
CREATE INDEX idx_customers_subscription_tier ON customers(subscription_tier);
CREATE INDEX idx_customer_events_date ON customer_events(event_date);
CREATE INDEX idx_customer_events_customer ON customer_events(customer_id);
