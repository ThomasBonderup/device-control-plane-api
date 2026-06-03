CREATE TABLE IF NOT EXISTS assets (
  id BIGSERIAL PRIMARY KEY,
  company_id BIGINT NOT NULL,
  asset_type TEXT NOT NULL,
  name TEXT NOT NULL,
  external_ref TEXT NOT NULL,
  parent_asset_id BIGINT,
  serial_number TEXT,
  hardware_model TEXT,
  protocol TEXT,
  site_label TEXT,
  metadata_json TEXT,
  is_deleted BOOLEAN NOT NULL DEFAULT FALSE
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_assets_company_name_active
  ON assets(company_id, lower(name))
  WHERE is_deleted = FALSE;
