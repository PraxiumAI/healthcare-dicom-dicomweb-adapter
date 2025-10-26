-- Database initialization script for DICOM Adapter Dynamic Configuration
-- This script creates tables and indexes for:
-- 1. Device Authorization
-- 2. AET Storage Mapping
-- 3. Study Storage Mapping
-- Create Device Authorization table
-- Stores authorized calling AET and called AET pairs
CREATE TABLE IF NOT EXISTS dicom_device (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    calling_aet VARCHAR(16) NOT NULL,
    called_aet VARCHAR(16) NOT NULL,
    name VARCHAR(255),
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (calling_aet, called_aet)
);


CREATE TABLE IF NOT EXISTS dicom_destination (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    dicomweb_destination VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (dicomweb_destination)
);


-- Defines default storage destinations per device
CREATE TABLE IF NOT EXISTS dicom_device_destination (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    device_id UUID NOT NULL,
    dicomweb_destination_id UUID NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_device_destination_device FOREIGN KEY (device_id) REFERENCES dicom_device(id) ON DELETE CASCADE ON UPDATE CASCADE
    CONSTRAINT fk_device_destination_destination FOREIGN KEY (dicomweb_destination) REFERENCES dicom_destination(id) ON DELETE CASCADE ON UPDATE CASCADE
);

-- Create Study Storage Mapping table
-- Auto-populated by adapter when first object of a study is received
CREATE TABLE IF NOT EXISTS dicom_study_destination (
    study_uid VARCHAR(64) PRIMARY KEY,
    dicomweb_destination_id UUID NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_study_destination_destination FOREIGN KEY (dicomweb_destination) REFERENCES dicom_destination(id) ON DELETE CASCADE ON UPDATE CASCADE
);

-- Create trigger to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Apply trigger to all tables
CREATE TRIGGER update_device_updated_at BEFORE
UPDATE
    ON dicom_device FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_device_destination_updated_at BEFORE
UPDATE
    ON dicom_device_destination FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_study_destination_updated_at BEFORE
UPDATE
    ON dicom_study_destination FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

