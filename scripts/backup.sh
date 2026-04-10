#!/bin/bash

DATE=$(date +%Y-%m-%d_%H-%M-%S)
BACKUP_DIR=/opt/airflow/backups

mkdir -p $BACKUP_DIR

echo "Starting backup..."

# MRR

docker exec postgres pg_dump -U airflow mrr > $BACKUP_DIR/mrr_$DATE.sql

# STG

docker exec postgres pg_dump -U airflow stg > $BACKUP_DIR/stg_$DATE.sql

# DWH

docker exec postgres pg_dump -U airflow dwh > $BACKUP_DIR/dwh_$DATE.sql

echo "Backup completed!"
