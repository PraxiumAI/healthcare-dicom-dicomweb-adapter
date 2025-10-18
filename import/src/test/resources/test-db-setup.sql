-- Integration test database setup for praxium_dev
-- This script creates test schema and inserts test data

-- Drop existing test tables if they exist
DROP TABLE IF EXISTS study_storage CASCADE;
DROP TABLE IF EXISTS aet_storage CASCADE;
DROP TABLE IF EXISTS aet_authorization CASCADE;

-- Create AET Authorization table
CREATE TABLE aet_authorization (
    calling_aet VARCHAR(16) PRIMARY KEY,
    called_aet VARCHAR(16) NOT NULL
);

CREATE INDEX idx_aet_authorization_called ON aet_authorization(called_aet);

-- Create AET Storage Mapping table
CREATE TABLE aet_storage (
    aet VARCHAR(16) PRIMARY KEY,
    dicomweb_destination VARCHAR(255) NOT NULL
);

-- Create Study Storage Mapping table
CREATE TABLE study_storage (
    study_uid VARCHAR(64) PRIMARY KEY,
    dicomweb_destination VARCHAR(255) NOT NULL
);

-- Insert test authorization data
INSERT INTO aet_authorization (calling_aet, called_aet) VALUES
    ('TEST_HOSPITAL_A', 'TEST_PRAXIUM'),
    ('TEST_HOSPITAL_B', 'TEST_PRAXIUM'),
    ('TEST_MODALITY_01', 'TEST_PRAXIUM'),
    ('UNAUTHORIZED_AET', 'WRONG_AET');

-- Insert test AET storage mappings
INSERT INTO aet_storage (aet, dicomweb_destination) VALUES
    ('TEST_HOSPITAL_A', 'https://healthcare.googleapis.com/v1/projects/test-project-a/locations/us-central1/datasets/test-dataset-a/dicomStores/test-store-a/dicomWeb'),
    ('TEST_HOSPITAL_B', 'https://healthcare.googleapis.com/v1/projects/test-project-b/locations/us-central1/datasets/test-dataset-b/dicomStores/test-store-b/dicomWeb');

-- Insert test study storage mappings
INSERT INTO study_storage (study_uid, dicomweb_destination) VALUES
    ('1.2.3.4.5.6.7.8.9.TEST_STUDY_1', 'https://healthcare.googleapis.com/v1/projects/test-project-a/locations/us-central1/datasets/test-dataset-a/dicomStores/test-store-a/dicomWeb'),
    ('1.2.3.4.5.6.7.8.9.TEST_STUDY_2', 'https://healthcare.googleapis.com/v1/projects/test-project-b/locations/us-central1/datasets/test-dataset-b/dicomStores/test-store-b/dicomWeb');

-- Verify data
SELECT 'Authorization records:' as info, COUNT(*) as count FROM aet_authorization
UNION ALL
SELECT 'AET storage records:', COUNT(*) FROM aet_storage
UNION ALL
SELECT 'Study storage records:', COUNT(*) FROM study_storage;
