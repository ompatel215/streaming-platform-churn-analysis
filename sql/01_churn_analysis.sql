-- ============================================================================
-- QUERY 1: Monthly Churn Rate by Subscription Tier
-- ============================================================================
WITH monthly_churn AS (
    SELECT
        DATE_TRUNC('month', COALESCE(churn_date, acquisition_date))::DATE AS month,
        subscription_tier,
        COUNT(CASE WHEN churn = 'Yes' THEN 1 END) AS churned_count,
        COUNT(*) AS total_customers,
        ROUND(100.0 * COUNT(CASE WHEN churn = 'Yes' THEN 1 END) / COUNT(*), 2) AS churn_rate_pct,
        SUM(CASE WHEN churn = 'Yes' THEN total_charges ELSE 0 END) AS churned_revenue,
        AVG(tenure_months) AS avg_tenure_months
    FROM customers
    GROUP BY DATE_TRUNC('month', COALESCE(churn_date, acquisition_date))::DATE, subscription_tier
)
SELECT
    month,
    subscription_tier,
    churned_count,
    total_customers,
    churn_rate_pct,
    churned_revenue,
    avg_tenure_months
FROM monthly_churn
ORDER BY month DESC, subscription_tier;


-- ============================================================================
-- QUERY 2: Revenue at Risk from Churned Subscribers
-- ============================================================================
WITH churned_analysis AS (
    SELECT
        subscription_tier,
        COUNT(*) AS churned_customers,
        SUM(monthly_charges) AS monthly_revenue_lost,
        SUM(monthly_charges) * 12 AS annual_revenue_at_risk,
        SUM(total_charges) AS lifetime_revenue_lost,
        AVG(tenure_months) AS avg_tenure_before_churn,
        AVG(total_charges) AS avg_customer_ltv
    FROM customers
    WHERE churn = 'Yes'
    GROUP BY subscription_tier
)
SELECT
    subscription_tier,
    churned_customers,
    ROUND(monthly_revenue_lost, 2) AS monthly_revenue_lost,
    ROUND(annual_revenue_at_risk, 2) AS annual_revenue_at_risk,
    ROUND(lifetime_revenue_lost, 2) AS lifetime_revenue_lost,
    ROUND(avg_tenure_before_churn, 1) AS avg_tenure_before_churn,
    ROUND(avg_customer_ltv, 2) AS avg_customer_ltv
FROM churned_analysis
ORDER BY annual_revenue_at_risk DESC;


-- ============================================================================
-- QUERY 3: Cohort Survival Analysis (3/6/12 month retention)
-- ============================================================================
WITH customer_cohorts AS (
    SELECT
        customer_id,
        DATE_TRUNC('month', acquisition_date)::DATE AS cohort_month,
        EXTRACT(YEAR FROM AGE(CURRENT_DATE, acquisition_date)) * 12 +
        EXTRACT(MONTH FROM AGE(CURRENT_DATE, acquisition_date)) AS months_active,
        churn,
        acquisition_channel,
        subscription_tier
    FROM customers
),
cohort_summary AS (
    SELECT
        cohort_month,
        acquisition_channel,
        subscription_tier,
        COUNT(*) AS cohort_size,
        COUNT(CASE WHEN months_active >= 3 AND churn = 'No' THEN 1 END) AS active_at_3mo,
        COUNT(CASE WHEN months_active >= 6 AND churn = 'No' THEN 1 END) AS active_at_6mo,
        COUNT(CASE WHEN months_active >= 12 AND churn = 'No' THEN 1 END) AS active_at_12mo,
        COUNT(CASE WHEN churn = 'Yes' THEN 1 END) AS churned_count
    FROM customer_cohorts
    GROUP BY cohort_month, acquisition_channel, subscription_tier
)
SELECT
    cohort_month,
    acquisition_channel,
    subscription_tier,
    cohort_size,
    ROUND(100.0 * active_at_3mo / cohort_size, 1) AS retention_3mo_pct,
    ROUND(100.0 * active_at_6mo / cohort_size, 1) AS retention_6mo_pct,
    ROUND(100.0 * active_at_12mo / cohort_size, 1) AS retention_12mo_pct,
    ROUND(100.0 * churned_count / cohort_size, 1) AS overall_churn_pct
FROM cohort_summary
WHERE cohort_month >= CURRENT_DATE - INTERVAL '24 months'
ORDER BY cohort_month DESC, subscription_tier;


-- ============================================================================
-- QUERY 4: Average Subscriber Lifetime Value by Acquisition Channel
-- ============================================================================
SELECT
    acquisition_channel,
    subscription_tier,
    COUNT(*) AS total_customers,
    COUNT(CASE WHEN churn = 'Yes' THEN 1 END) AS churned_customers,
    ROUND(100.0 * COUNT(CASE WHEN churn = 'Yes' THEN 1 END) / COUNT(*), 1) AS churn_rate_pct,
    ROUND(AVG(tenure_months), 1) AS avg_tenure_months,
    ROUND(AVG(total_charges), 2) AS avg_ltv,
    ROUND(SUM(total_charges), 2) AS total_ltv,
    ROUND(AVG(monthly_charges), 2) AS avg_monthly_charges,
    ROUND(AVG(tenure_months) * AVG(monthly_charges), 2) AS predicted_ltv
FROM customers
GROUP BY acquisition_channel, subscription_tier
ORDER BY total_ltv DESC;
