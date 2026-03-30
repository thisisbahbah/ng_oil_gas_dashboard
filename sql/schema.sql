DROP TABLE IF EXISTS production_by_field CASCADE;
DROP TABLE IF EXISTS national_production CASCADE;
DROP TABLE IF EXISTS brent_prices CASCADE;
DROP TABLE IF EXISTS opec_quotas CASCADE;

CREATE TABLE brent_prices (
id          SERIAL PRIMARY KEY,
price_date  DATE NOT NULL UNIQUE,
price_usd   NUMERIC(10,2) NOT NULL,
source      VARCHAR(50) DEFAULT 'EIA',
created_at  TIMESTAMP DEFAULT NOW()
);
CREATE INDEX idx_brent_date ON brent_prices(price_date);

CREATE TABLE national_production (
id                SERIAL PRIMARY KEY,
production_month  DATE NOT NULL UNIQUE,
production_kbd    NUMERIC(10,2),
source            VARCHAR(50) DEFAULT 'EIA',
created_at        TIMESTAMP DEFAULT NOW()
);
CREATE INDEX idx_natprod_month ON national_production(production_month);

CREATE TABLE opec_quotas (
id           SERIAL PRIMARY KEY,
quota_month  DATE NOT NULL UNIQUE,
quota_kbd    NUMERIC(10,2),
actual_kbd   NUMERIC(10,2),
source       VARCHAR(50) DEFAULT 'OPEC',
created_at   TIMESTAMP DEFAULT NOW()
);

CREATE TABLE production_by_field (
id                SERIAL PRIMARY KEY,
production_month  DATE NOT NULL,
field_name        VARCHAR(100) NOT NULL,
operator          VARCHAR(100),
crude_grade       VARCHAR(50),
production_kbd    NUMERIC(10,3),
nameplate_kbd     NUMERIC(10,3),
shut_in_kbd       NUMERIC(10,3) GENERATED ALWAYS AS
(CASE WHEN nameplate_kbd IS NOT NULL
THEN GREATEST(nameplate_kbd - production_kbd, 0)
ELSE NULL END) STORED,
shut_in_reason    VARCHAR(200),
source            VARCHAR(50) DEFAULT 'NUPRC',
created_at        TIMESTAMP DEFAULT NOW(),
UNIQUE(production_month, field_name)
);
CREATE INDEX idx_field_month ON production_by_field(production_month);
CREATE INDEX idx_field_name ON production_by_field(field_name);

DO $$ BEGIN
RAISE NOTICE 'Schema created: brent_prices, national_production, opec_quotas, production_by_field';
END $$;