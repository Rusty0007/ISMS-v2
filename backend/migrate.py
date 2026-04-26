"""
ISMS Database Migration Runner
================================
Run this BEFORE starting the backend in production so that schema changes
(ADD COLUMN IF NOT EXISTS, CREATE TABLE IF NOT EXISTS, etc.) are applied
while no traffic is being served. This avoids the brief ACCESS EXCLUSIVE
locks that the inline startup migrations would otherwise acquire on a live
database.

Usage:
    python migrate.py

Exit codes:
    0 — migrations applied successfully
    1 — migration failed (check stderr)

In docker-compose or Kubernetes, run this as an init container / entrypoint
step before the backend process starts.
"""

import logging
import sys

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("migrate")


def main() -> int:
    logger.info("=== ISMS migration runner starting ===")
    try:
        # Import triggers app startup validations; we need only the migration
        # functions, so import them directly.
        from app.database import engine
        from app.models import models

        logger.info("Creating base schema tables (idempotent)...")
        models.Base.metadata.create_all(bind=engine)

        logger.info("Running column migrations...")
        from app.main import (
            _run_column_migrations,
            _run_competitive_tier_setup,
            _run_stored_procedures,
            _run_view_setup,
        )
        _run_column_migrations()
        logger.info("Column migrations complete.")

        _run_competitive_tier_setup()
        logger.info("Competitive tier setup complete.")

        _run_stored_procedures()
        logger.info("Stored procedures updated.")

        _run_view_setup()
        logger.info("Views rebuilt.")

    except Exception as exc:
        logger.error(f"Migration FAILED: {exc}", exc_info=True)
        return 1

    logger.info("=== All migrations applied successfully ===")
    return 0


if __name__ == "__main__":
    sys.exit(main())
