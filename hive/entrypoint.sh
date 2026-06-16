#!/bin/bash

DB_TYPE="postgres"

echo ">>> Waiting for hive-postgres to be ready..."
until PGPASSWORD=hive psql -h hive-postgres -U hive -d metastore -c '\q' > /dev/null 2>&1; do
    echo ">>> hive-postgres not ready yet, retrying in 3s..."
    sleep 3
done

echo ">>> hive-postgres is ready!"

echo ">>> Checking Hive Metastore schema..."

SCHEMA_INFO=$(schematool -dbType ${DB_TYPE} -info 2>&1)
SCHEMA_EXIT=$?

echo "$SCHEMA_INFO"

if [ $SCHEMA_EXIT -eq 0 ]; then
    echo ">>> Schema already initialized."
else
    echo ">>> Schema not found. Initializing..."
    schematool -dbType ${DB_TYPE} -initSchema
fi

echo ">>> Starting Hive Metastore service..."
exec hive --service metastore