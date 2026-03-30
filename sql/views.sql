-- ============================================================
-- Nigeria Oil & Gas Dashboard — Analytics Views
-- Run after schema.sql:
--   psql -U postgres -d ng_oil_gas -f sql/views.sql
-- ============================================================

-- ── 1. Monthly production trend (national) ────────────────
-- Joins EIA production with Brent price for correlation analysis
CREATE OR REPLACE VIEW v_monthly_production_price AS
SELECT
    np.production_month,
    np.production_kbd,
    bp.price_usd AS brent_price_usd,
    LAG(np.production_kbd, 12) OVER (ORDER BY np.production_month)
        AS production_kbd_prev_year,
    ROUND(
        (np.production_kbd - LAG(np.production_kbd, 12)
            OVER (ORDER BY np.production_month))
        / NULLIF(LAG(np.production_kbd, 12)
            OVER (ORDER BY np.production_month), 0) * 100,
        2
    ) AS yoy_change_pct
FROM national_production np
LEFT JOIN brent_prices bp
    ON DATE_TRUNC('month', bp.price_date) = np.production_month
ORDER BY np.production_month;


-- ── 2. OPEC compliance view ───────────────────────────────
CREATE OR REPLACE VIEW v_opec_compliance AS
SELECT
    oq.quota_month,
    oq.quota_kbd,
    oq.actual_kbd,
    np.production_kbd AS eia_production_kbd,
    ROUND(oq.actual_kbd - oq.quota_kbd, 2) AS vs_quota_kbd,
    ROUND((oq.actual_kbd / NULLIF(oq.quota_kbd, 0) - 1) * 100, 2) AS compliance_pct
FROM opec_quotas oq
LEFT JOIN national_production np
    ON np.production_month = oq.quota_month
ORDER BY oq.quota_month;


-- ── 3. Top fields by production (latest 12 months) ────────
CREATE OR REPLACE VIEW v_top_fields_recent AS
SELECT
    field_name,
    operator,
    crude_grade,
    ROUND(AVG(production_kbd), 2)   AS avg_production_kbd,
    ROUND(AVG(shut_in_kbd), 2)      AS avg_shut_in_kbd,
    ROUND(AVG(nameplate_kbd), 2)    AS avg_nameplate_kbd,
    ROUND(
        AVG(shut_in_kbd) / NULLIF(AVG(nameplate_kbd), 0) * 100,
        1
    )                                AS avg_shut_in_pct,
    COUNT(*)                         AS months_reported
FROM production_by_field
WHERE production_month >= DATE_TRUNC('month', NOW()) - INTERVAL '12 months'
GROUP BY field_name, operator, crude_grade
ORDER BY avg_production_kbd DESC;


-- ── 4. Shut-in analysis by field and time ─────────────────
CREATE OR REPLACE VIEW v_shutin_detail AS
SELECT
    production_month,
    field_name,
    operator,
    crude_grade,
    production_kbd,
    nameplate_kbd,
    shut_in_kbd,
    shut_in_reason,
    ROUND(shut_in_kbd / NULLIF(nameplate_kbd, 0) * 100, 1) AS shut_in_pct
FROM production_by_field
WHERE shut_in_kbd > 0
ORDER BY production_month DESC, shut_in_kbd DESC;


-- ── 5. National shut-in aggregate by month ────────────────
CREATE OR REPLACE VIEW v_national_shutin_monthly AS
SELECT
    production_month,
    ROUND(SUM(production_kbd), 2)   AS total_production_kbd,
    ROUND(SUM(nameplate_kbd), 2)    AS total_nameplate_kbd,
    ROUND(SUM(shut_in_kbd), 2)      AS total_shutin_kbd,
    ROUND(
        SUM(shut_in_kbd) / NULLIF(SUM(nameplate_kbd), 0) * 100,
        1
    )                                AS national_shutin_pct,
    COUNT(DISTINCT field_name)       AS fields_reporting
FROM production_by_field
WHERE nameplate_kbd IS NOT NULL
GROUP BY production_month
ORDER BY production_month;


-- ── 6. Brent rolling correlation (12-month window) ────────
-- CORR() as a window function is not supported in all PostgreSQL
-- versions. This version calculates it via a self-join instead.
CREATE OR REPLACE VIEW v_price_production_rolling AS
WITH base AS (
    SELECT
        production_month,
        production_kbd,
        brent_price_usd,
        ROW_NUMBER() OVER (ORDER BY production_month) AS rn
    FROM v_monthly_production_price
    WHERE production_kbd IS NOT NULL
      AND brent_price_usd IS NOT NULL
)
SELECT
    b.production_month,
    b.production_kbd,
    b.brent_price_usd,
    ROUND(
        CORR(w.production_kbd, w.brent_price_usd)::NUMERIC,
        4
    ) AS rolling_12m_corr
FROM base b
JOIN base w
    ON w.rn BETWEEN b.rn - 11 AND b.rn
GROUP BY b.production_month, b.production_kbd, b.brent_price_usd, b.rn
ORDER BY b.production_month;


-- ── Confirmation ──────────────────────────────────────────
DO $$
BEGIN
    RAISE NOTICE 'Views created: v_monthly_production_price, v_opec_compliance,';
    RAISE NOTICE '               v_top_fields_recent, v_shutin_detail,';
    RAISE NOTICE '               v_national_shutin_monthly, v_price_production_rolling';
END $$;