from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from jose import jwt, JWTError
from sqlalchemy.orm import Session
from app.config import settings
from app.database import get_db
from app.models.models import Profile, UserRoleModel

bearer_scheme = HTTPBearer()

def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(bearer_scheme),
    db: Session = Depends(get_db),
) -> dict:
    """
    Extracts and verifies the JWT from the Authorization header.
    Also checks token_version against the DB to invalidate old sessions.
    Raises HTTP 401 if the token is missing, expired, invalid, or superseded.
    """
    token = credentials.credentials
    try:
        payload = jwt.decode(
            token,
            settings.jwt_secret,
            algorithms=[settings.jwt_algorithm],
            options={"verify_aud": False},
        )
        user_id: str | None = payload.get("sub")
        if not user_id:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Token is missing user ID",
                headers={"WWW-Authenticate": "Bearer"},
            )

        # Verify token_version — rejects tokens from previous sessions
        token_version: int = payload.get("tv", 0)
        profile = db.query(Profile).filter(Profile.id == user_id).first()
        if profile is None or int(getattr(profile, "token_version", 0) or 0) != token_version:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Session expired. Please log in again.",
                headers={"WWW-Authenticate": "Bearer"},
            )

        return {"id": user_id, "email": payload.get("email")}
    except JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Could not validate credentials",
            headers={"WWW-Authenticate": "Bearer"},
        )


def require_role(required_role: str):
    def role_checker(
        current_user: dict = Depends(get_current_user),
        db: Session = Depends(get_db),
    ) -> dict:
        roles = db.query(UserRoleModel.role) \
            .filter(UserRoleModel.user_id == current_user["id"]) \
            .all()
        user_roles = [r.role.value for r in roles]
        if required_role not in user_roles:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"This action requires the '{required_role}' role.",
            )
        return {**current_user, "roles": user_roles}
    return role_checker