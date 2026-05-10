#!/bin/sh
# ISMS Database Backup Script
# Runs inside the db-backup container. Called once on startup, then every 24h.
# Backups are stored in /backups with a timestamp suffix.
# Keeps the last 7 backups; older ones are deleted automatically.

BACKUP_DIR="/backups"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
FILENAME="isms_backup_${TIMESTAMP}.sql.gz"
KEEP_LAST=7

echo "[backup] Starting backup at ${TIMESTAMP}"

pg_dump \
  --host="${PGHOST}" \
  --username="${PGUSER}" \
  --dbname="${PGDATABASE}" \
  --no-password \
  --format=plain \
  --no-owner \
  --no-acl \
  | gzip > "${BACKUP_DIR}/${FILENAME}"

echo "[backup] Backup written: ${FILENAME}"

# Remove backups older than KEEP_LAST (sorted by name = by date)
BACKUP_COUNT=$(ls -1 "${BACKUP_DIR}"/isms_backup_*.sql.gz 2>/dev/null | wc -l)
if [ "${BACKUP_COUNT}" -gt "${KEEP_LAST}" ]; then
    REMOVE_COUNT=$((BACKUP_COUNT - KEEP_LAST))
    ls -1t "${BACKUP_DIR}"/isms_backup_*.sql.gz | tail -n "${REMOVE_COUNT}" | xargs rm -f
    echo "[backup] Removed ${REMOVE_COUNT} old backup(s). Keeping ${KEEP_LAST}."
fi

echo "[backup] Done. Total backups: $(ls -1 ${BACKUP_DIR}/isms_backup_*.sql.gz 2>/dev/null | wc -l)"
