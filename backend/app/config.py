from pydantic_settings import BaseSettings, SettingsConfigDict

class Settings(BaseSettings):

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    # --- PostgreSQL ---
    database_url: str

    # --- JWT ---
    jwt_secret: str
    jwt_algorithm: str = "HS256"
    access_token_expire_minutes: int = 1440

    # --- Redis ---
    redis_url: str = "redis://redis:6379"

    # --- App ---
    app_env: str = "development"
    allowed_origins: str = "http://localhost:3000"

    # --- Anthropic (LLM insights) ---
    anthropic_api_key: str = ""

    # --- Firebase Cloud Messaging ---
    firebase_credentials_path: str = "firebase-credentials.json"

    @property
    def allowed_origins_list(self) -> list[str]:
        return [o.strip() for o in self.allowed_origins.split(",")]


settings = Settings()  # type: ignore[call-arg]