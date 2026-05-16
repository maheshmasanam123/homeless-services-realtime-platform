#!/usr/bin/env bash
# Nightly Sqoop import of agency reference data into HDFS, scheduled by Oozie
# and monitored via Control-M (legacy CDH bridge).

set -euo pipefail

CONN="jdbc:oracle:thin:@//agency-db:1521/HMIS"
USER="${SQOOP_USER:-svc_etl}"
PW_FILE="${SQOOP_PW_FILE:-/etc/secrets/sqoop.pw}"
HDFS_BASE="/raw/hmis"

for TBL in clients projects enrollments services; do
  sqoop import \
    --connect "$CONN" \
    --username "$USER" \
    --password-file "$PW_FILE" \
    --table "$TBL" \
    --target-dir "$HDFS_BASE/$TBL/$(date +%Y%m%d)" \
    --as-parquetfile \
    --compress --compression-codec snappy \
    --num-mappers 4 \
    --null-string '\\N' --null-non-string '\\N'
done
