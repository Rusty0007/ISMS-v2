"""
Seed script — grants system_admin role to an existing ISMS user.

Usage (run from inside the backend container or with the correct DATABASE_URL):
    python -m app.scripts.create_admin --email user@example.com
"""

import argparse
import sys

from sqlalchemy.orm import Session

from app.database import SessionLocal
from app.models.models import Profile, UserRole, UserRoleModel


def create_admin(email: str) -> None:
    db: Session = SessionLocal()
    try:
        profile = db.query(Profile).filter(Profile.email == email).first()
        if not profile:
            print(f"[ERROR] No user found with email '{email}'.")
            sys.exit(1)

        existing = (
            db.query(UserRoleModel)
            .filter(
                UserRoleModel.user_id == str(profile.id),
                UserRoleModel.role    == UserRole.system_admin,
            )
            .first()
        )
        if existing:
            print(f"[INFO] '{profile.username}' ({email}) already has system_admin.")
            return

        db.add(UserRoleModel(user_id=str(profile.id), role=UserRole.system_admin))
        db.commit()
        print(f"[OK] '{profile.username}' ({email}) is now a system_admin.")
    finally:
        db.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Grant system_admin role to a user")
    parser.add_argument("--email", required=True, help="Email of the user to promote")
    args = parser.parse_args()
    create_admin(args.email)
