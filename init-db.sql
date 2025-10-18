-- Database initialization script for DICOM Adapter Dynamic Configuration
-- This script creates tables and indexes for:
-- 1. AET Authorization
-- 2. AET Storage Mapping
-- 3. Study Storage Mapping
-- Create AET Authorization table
-- Stores authorized calling AET and called AET pairs
CREATE TABLE IF NOT EXISTS aet_authorization (
    calling_aet VARCHAR(16) PRIMARY KEY,
    called_aet VARCHAR(16) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index for searching by called_aet
CREATE INDEX IF NOT EXISTS idx_aet_authorization_called ON aet_authorization(called_aet);

-- Comment table
COMMENT ON TABLE aet_authorization IS 'Authorized DICOM AET pairs for incoming associations';

COMMENT ON COLUMN aet_authorization.calling_aet IS 'Calling Application Entity Title from client';

COMMENT ON COLUMN aet_authorization.called_aet IS 'Called Application Entity Title (adapter AET)';

-- Create AET Storage Mapping table
-- Defines default storage destinations per calling AET
CREATE TABLE IF NOT EXISTS aet_storage (
    aet VARCHAR(16) PRIMARY KEY,
    dicomweb_destination VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Comment table
COMMENT ON TABLE aet_storage IS 'Default DICOMweb storage destination per AET';

COMMENT ON COLUMN aet_storage.aet IS 'Application Entity Title';

COMMENT ON COLUMN aet_storage.dicomweb_destination IS 'Full DICOMweb URL (e.g., https://...../dicomWeb)';

-- Create Study Storage Mapping table
-- Auto-populated by adapter when first object of a study is received
CREATE TABLE IF NOT EXISTS study_storage (
    study_uid VARCHAR(64) PRIMARY KEY,
    dicomweb_destination VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Comment table
COMMENT ON TABLE study_storage IS 'Study-to-storage mapping, automatically populated by adapter';

COMMENT ON COLUMN study_storage.study_uid IS 'DICOM StudyInstanceUID';

COMMENT ON COLUMN study_storage.dicomweb_destination IS 'DICOMweb URL where study is stored';

-- Create trigger to update updated_at timestamp
CREATE
OR REPLACE FUNCTION update_updated_at_column() RETURNS TRIGGER AS $ $ BEGIN NEW.updated_at = CURRENT_TIMESTAMP;

RETURN NEW;

END;

$ $ language 'plpgsql';

-- Apply trigger to all tables
CREATE TRIGGER update_aet_authorization_updated_at BEFORE
UPDATE
    ON aet_authorization FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_aet_storage_updated_at BEFORE
UPDATE
    ON aet_storage FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_study_storage_updated_at BEFORE
UPDATE
    ON study_storage FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Insert example data (OPTIONAL - comment out for production)
-- Example 1: Hospital A
-- INSERT INTO aet_authorization (calling_aet, called_aet)
-- VALUES ('HOSPITAL_A', 'PRAXIUM')
-- ON CONFLICT (calling_aet) DO NOTHING;
-- INSERT INTO aet_storage (aet, dicomweb_destination)
-- VALUES ('HOSPITAL_A', 'https://healthcare.googleapis.com/v1/projects/project-a/locations/us-central1/datasets/dataset-a/dicomStores/store-a/dicomWeb')
-- ON CONFLICT (aet) DO NOTHING;
-- -- Example 2: Hospital B
-- INSERT INTO aet_authorization (calling_aet, called_aet)
-- VALUES ('HOSPITAL_B', 'PRAXIUM')
-- ON CONFLICT (calling_aet) DO NOTHING;
-- INSERT INTO aet_storage (aet, dicomweb_destination)
-- VALUES ('HOSPITAL_B', 'https://healthcare.googleapis.com/v1/projects/project-b/locations/us-central1/datasets/dataset-b/dicomStores/store-b/dicomWeb')
-- ON CONFLICT (aet) DO NOTHING;
-- -- Example 3: Modality 01
-- INSERT INTO aet_authorization (calling_aet, called_aet)
-- VALUES ('MODALITY_01', 'PRAXIUM')
-- ON CONFLICT (calling_aet) DO NOTHING;
-- Display summary
DO $ $ BEGIN RAISE NOTICE '=====================================';

RAISE NOTICE 'Database initialization complete!';

RAISE NOTICE '=====================================';

RAISE NOTICE 'Tables created:';

RAISE NOTICE '  - aet_authorization';

RAISE NOTICE '  - aet_storage';

RAISE NOTICE '  - study_storage';

RAISE NOTICE '';

RAISE NOTICE 'Next steps:';

RAISE NOTICE '  1. Review and update authorization rules';

RAISE NOTICE '  2. Configure AET storage destinations';

RAISE NOTICE '  3. Start DICOM adapter with --db_url flag';

RAISE NOTICE '=====================================';

END $ $;