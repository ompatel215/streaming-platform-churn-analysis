-- ============================================================================
-- QUERY 5: Month-over-Month Growth vs Cancellation Rate (Using LAG)
-- ============================================================================
WITH monthly_summary AS (
    SELECT
        DATE_TRUNC('month', acquisition_date)::DATE AS month,
        subscription_tier,
        COUNT(*) AS new_signups,
        0 AS cancellations
    FROM customers
    WHERE churn = 'No'
    GROUP BY DATE_TRUNC('month', acquisition_date)::DATE, subscription_tier

    UNION ALL

    SELECT
        DATE_TRUNC('month', churn_date)::DATE AS month,
        subscription_tier,
        0 AS new_signups,
        COUNT(*) AS cancellations
    FROM customers
    WHERE churn = 'Yes'
    GROUP BY DATE_TRUNC('month', churn_date)::DATE, subscription_tier
),
monthly_net AS (
    SELECT
        month,
        subscription_tier,
        SUM(new_signups) AS new_signups,
        SUM(cancellations) AS cancellations
    FROM monthly_summary
    GROUP BY month, subscription_tier
),
with_lag AS (
    SELECT
        month,
        subscription_tier,
        new_signups,
        cancellations,
        new_signups - cancellations AS net_growth,
        LAG(new_signups - cancellations) OVER (PARTITION BY subscription_tier ORDER BY month) AS prev_month_growth,
        LAG(new_signups) OVER (PARTITION BY subscription_tier ORDER BY month) AS prev_month_signups,
        LAG(cancellations) OVER (PARTITION BY subscription_tier ORDER BY month) AS prev_month_cancellations
    FROM monthly_net
)
SELECT
    month,
    subscription_tier,
    new_signups,
    cancellations,
    net_growth,
    prev_month_growth,
    CASE
        WHEN prev_month_growth IS NULL THEN NULL
        ELSE ROUND(100.0 * (net_growth - prev_month_growth) / ABS(prev_month_growth), 2)
    END AS mom_growth_rate_pct,
    CASE
        WHEN new_signups > 0 THEN ROUND(100.0 * cancellations / (new_signups + cancellations), 2)
        ELSE NULL
    END AS cancellation_rate_pct
FROM with_lag
ORDER BY month DESC, subscription_tier;


-- ============================================================================
-- QUERY 6: Top Churn Predictors by Segment
-- ============================================================================
WITH churn_by_contract AS (
    SELECT
        'contract_type' AS predictor_category,
        contract AS predictor_value,
        COUNT(*) AS total_customers,
        COUNT(CASE WHEN churn = 'Yes' THEN 1 END) AS churned_count,
        ROUND(100.0 * COUNT(CASE WHEN churn = 'Yes' THEN 1 END) / COUNT(*), 2) AS churn_rate_pct,
        ROUND(AVG(tenure_months), 1) AS avg_tenure,
        ROUND(AVG(monthly_charges), 2) AS avg_monthly_charges
    FROM customers
    GROUP BY contract
),
churn_by_payment AS (
    SELECT
        'payment_method' AS predictor_category,
        payment_method AS predictor_value,
        COUNT(*) AS total_customers,
        COUNT(CASE WHEN churn = 'Yes' THEN 1 END) AS churned_count,
        ROUND(100.0 * COUNT(CASE WHEN churn = 'Yes' THEN 1 END) / COUNT(*), 2) AS churn_rate_pct,
        ROUND(AVG(tenure_months), 1) AS avg_tenure,
        ROUND(AVG(monthly_charges), 2) AS avg_monthly_charges
    FROM customers
    GROUP BY payment_method
),
churn_by_tenure_cohort AS (
    SELECT
        'tenure_cohort' AS predictor_category,
        CASE
            WHEN tenure_months <= 6 THEN '0-6 months'
            WHEN tenure_months <= 12 THEN '7-12 months'
            WHEN tenure_months <= 24 THEN '13-24 months'
            ELSE '25+ months'
        END AS predictor_value,
        COUNT(*) AS total_customers,
        COUNT(CASE WHEN churn = 'Yes' THEN 1 END) AS churned_count,
        ROUND(100.0 * COUNT(CASE WHEN churn = 'Yes' THEN 1 END) / COUNT(*), 2) AS churn_rate_pct,
        ROUND(AVG(tenure_months), 1) AS avg_tenure,
        ROUND(AVG(monthly_charges), 2) AS avg_monthly_charges
    FROM customers
    GROUP BY
        CASE
            WHEN tenure_months <= 6 THEN '0-6 months'
            WHEN tenure_months <= 12 THEN '7-12 months'
            WHEN tenure_months <= 24 THEN '13-24 months'
            ELSE '25+ months'
        END
),
churn_by_tech_support AS (
    SELECT
        'tech_support' AS predictor_category,
        COALESCE(tech_support, 'No') AS predictor_value,
        COUNT(*) AS total_customers,
        COUNT(CASE WHEN churn = 'Yes' THEN 1 END) AS churned_count,
        ROUND(100.0 * COUNT(CASE WHEN churn = 'Yes' THEN 1 END) / COUNT(*), 2) AS churn_rate_pct,
        ROUND(AVG(tenure_months), 1) AS avg_tenure,
        ROUND(AVG(monthly_charges), 2) AS avg_monthly_charges
    FROM customers
    GROUP BY COALESCE(tech_support, 'No')
)
SELECT * FROM churn_by_contract
UNION ALL
SELECT * FROM churn_by_payment
UNION ALL
SELECT * FROM churn_by_tenure_cohort
UNION ALL
SELECT * FROM churn_by_tech_support
ORDER BY churn_rate_pct DESC;


-- ============================================================================
-- QUERY 7: Churn Heatmap Data (by Cohort & Subscription Tier)
-- ============================================================================
WITH cohort_churn_grid AS (
    SELECT
        DATE_TRUNC('month', acquisition_date)::DATE AS cohort_month,
        subscription_tier,
        EXTRACT(YEAR FROM AGE(CURRENT_DATE, acquisition_date)) * 12 +
        EXTRACT(MONTH FROM AGE(CURRENT_DATE, acquisition_date)) AS months_since_acquisition,
        COUNT(*) AS cohort_count,
        COUNT(CASE WHEN churn = 'Yes' THEN 1 END) AS churned_in_cohort,
        ROUND(100.0 * COUNT(CASE WHEN churn = 'Yes' THEN 1 END) / COUNT(*), 1) AS churn_rate
    FROM customers
    GROUP BY
        DATE_TRUNC('month', acquisition_date)::DATE,
        subscription_tier,
        EXTRACT(YEAR FROM AGE(CURRENT_DATE, acquisition_date)) * 12 + EXTRACT(MONTH FROM AGE(CURRENT_DATE, acquisition_date))
)
SELECT
    cohort_month,
    subscription_tier,
    months_since_acquisition,
    cohort_count,
    churned_in_cohort,
    churn_rate
FROM cohort_churn_grid
WHERE cohort_month >= CURRENT_DATE - INTERVAL '36 months'
ORDER BY cohort_month DESC, months_since_acquisition;


-- ============================================================================
-- QUERY 8: Subscriber Funnel (Acquisition -> Active -> Churn)
-- ============================================================================
WITH funnel_data AS (
    SELECT
        subscription_tier,
        COUNT(*) AS total_acquired,
        COUNT(CASE WHEN churn = 'No' THEN 1 END) AS currently_active,
        COUNT(CASE WHEN churn = 'Yes' THEN 1 END) AS churned,
        ROUND(100.0 * COUNT(CASE WHEN churn = 'No' THEN 1 END) / COUNT(*), 1) AS retention_rate_pct
    FROM customers
    GROUP BY subscription_tier
)
SELECT
    subscription_tier,
    total_acquired,
    currently_active,
    churned,
    retention_rate_pct,
    ROUND(100.0 * currently_active / total_acquired, 1) AS active_pct,
    ROUND(100.0 * churned / total_acquired, 1) AS churn_pct
FROM funnel_data
ORDER BY total_acquired DESC;


-- ============================================================================
-- QUERY 9: Retention Curve by Plan Tier
-- ============================================================================
WITH month_grids AS (
    SELECT
        DATE_TRUNC('month', acquisition_date)::DATE AS acquisition_month,
        subscription_tier,
        GENERATE_SERIES(0, 24, 1) AS month_offset
    FROM (SELECT DISTINCT DATE_TRUNC('month', acquisition_date)::DATE, subscription_tier FROM customers) AS distinct_months
    CROSS JOIN LATERAL (SELECT 1) AS _
),
retention_by_month AS (
    SELECT
        mg.acquisition_month,
        mg.subscription_tier,
        mg.month_offset,
        COUNT(DISTINCT c.customer_id) AS cohort_size,
        COUNT(DISTINCT CASE
            WHEN c.churn = 'No'
            AND EXTRACT(YEAR FROM AGE(CURRENT_DATE, c.acquisition_date)) * 12 +
                EXTRACT(MONTH FROM AGE(CURRENT_DATE, c.acquisition_date)) >= mg.month_offset
            THEN c.customer_id
        END) AS surviving_customers,
        ROUND(100.0 * COUNT(DISTINCT CASE
            WHEN c.churn = 'No'
            AND EXTRACT(YEAR FROM AGE(CURRENT_DATE, c.acquisition_date)) * 12 +
                EXTRACT(MONTH FROM AGE(CURRENT_DATE, c.acquisition_date)) >= mg.month_offset
            THEN c.customer_id
        END) / COUNT(DISTINCT c.customer_id), 1) AS survival_rate_pct
    FROM month_grids mg
    LEFT JOIN customers c
        ON DATE_TRUNC('month', c.acquisition_date)::DATE = mg.acquisition_month
        AND c.subscription_tier = mg.subscription_tier
    GROUP BY mg.acquisition_month, mg.subscription_tier, mg.month_offset
)
SELECT
    acquisition_month,
    subscription_tier,
    month_offset,
    cohort_size,
    surviving_customers,
    survival_rate_pct
FROM retention_by_month
WHERE cohort_size > 0
ORDER BY acquisition_month DESC, month_offset;
