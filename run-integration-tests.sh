#!/bin/bash

# Integration Tests Runner Script
# This script runs integration tests against local PostgreSQL database

set -e

echo "========================================="
echo "DICOM Adapter Integration Tests"
echo "========================================="

# Database connection details
DB_HOST="${DB_HOST:-localhost}"
DB_PORT="${DB_PORT:-5432}"
DB_NAME="${DB_NAME:-praxium_dev}"
DB_USER="${DB_USER:-praxium}"
DB_PASSWORD="${DB_PASSWORD:-praxium}"

echo ""
echo "Database Configuration:"
echo "  Host: $DB_HOST"
echo "  Port: $DB_PORT"
echo "  Database: $DB_NAME"
echo "  User: $DB_USER"
echo ""

# Check if PostgreSQL is running
echo "Checking PostgreSQL connection..."
if psql -h "$DB_HOST" -U "$DB_USER" -d "$DB_NAME" -c "SELECT 1" > /dev/null 2>&1; then
    echo "✅ PostgreSQL connection successful"
else
    echo "❌ Cannot connect to PostgreSQL"
    echo "Please ensure PostgreSQL is running and credentials are correct"
    exit 1
fi

# Setup test database
echo ""
echo "Setting up test database schema..."
psql -h "$DB_HOST" -U "$DB_USER" -d "$DB_NAME" -f import/src/test/resources/test-db-setup.sql

echo ""
echo "Running integration tests..."
echo "========================================="

# Check if gradle wrapper exists
if [ -f "./gradlew" ]; then
    GRADLE_CMD="./gradlew"
elif command -v gradle &> /dev/null; then
    GRADLE_CMD="gradle"
else
    echo "❌ Gradle not found. Please install Gradle or use Docker to run tests."
    exit 1
fi

# Run integration tests
$GRADLE_CMD :import:test --tests DatabaseConfigServiceIntegrationTest --info

echo ""
echo "========================================="
echo "Integration Tests Complete!"
echo "========================================="
