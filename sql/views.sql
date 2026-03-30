CREATE OR REPLACE VIEW v_monthly_production_price AS
SELECT
np.production_month,
np.production_kbd,
bp.price_usd AS brent_price_usd,
ROUND(
(np.production_kbd - LAG(np.production_kbd, 12)
OVER (ORDER BY np.production_month))
/ NULLIF(LAG(np.production_kbd, 12)
OVER (ORDER BY np.production_month), 0) * 100, 2
) AS yoy_change_pct
FROM national_production np
LEFT JOIN brent_prices bp
ON DATE_TRUNC('month', bp.price_date) = np.production_month
ORDER BY np.production_month;

CREATE OR REPLACE VIEW v_opec_compliance AS
SELECT
oq.quota_month,
oq.quota_kbd,
oq.actual_kbd,
np.production_kbd AS eia_production_kbd,
ROUND(oq.actual_kbd - oq.quota_kbd, 2) AS vs_quota_kbd,
ROUND((oq.actual_kbd / NULLIF(oq.quota_kbd, 0) - 1) * 100, 2) AS compliance_pct
FROM opec_quotas oq
LEFT JOIN national_production np ON np.production_month = oq.quota_month
ORDER BY oq.quota_month;

CREATE OR REPLACE VIEW v_top_fields_recent AS
SELECT
field_name, operator, crude_grade,
ROUND(AVG(production_kbd), 2) AS avg_production_kbd,
ROUND(AVG(shut_in_kbd), 2) AS avg_shut_in_kbd,
ROUND(AVG(nameplate_kbd), 2) AS avg_nameplate_kbd,
ROUND(AVG(shut_in_kbd) / NULLIF(AVG(nameplate_kbd), 0) * 100, 1) AS avg_shut_in_pct,
COUNT(*) AS months_reported
FROM production_by_field
WHERE production_month >= DATE_TRUNC('month', NOW()) - INTERVAL '12 months'
GROUP BY field_name, operator, crude_grade
ORDER BY avg_production_kbd DESC;

CREATE OR REPLACE VIEW v_national_shutin_monthly AS
SELECT
production_month,
ROUND(SUM(production_kbd), 2) AS total_production_kbd,
ROUND(SUM(nameplate_kbd), 2) AS total_nameplate_kbd,
ROUND(SUM(shut_in_kbd), 2) AS total_shutin_kbd,
ROUND(SUM(shut_in_kbd) / NULLIF(SUM(nameplate_kbd), 0) * 100, 1) AS national_shutin_pct,
COUNT(DISTINCT field_name) AS fields_reporting
FROM production_by_field
WHERE nameplate_kbd IS NOT NULL
GROUP BY production_month
ORDER BY production_month;

DO $$ BEGIN
RAISE NOTICE 'Views created: v_monthly_production_price, v_opec_compliance, v_top_fields_recent, v_national_shutin_monthly';
END $$;