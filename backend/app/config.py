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
    match_approval_mode: str = "referee_only"

    # --- Anthropic (LLM insights) ---
    anthropic_api_key: str = ""

    # --- OpenRouter (preferred if configured for AI insights) ---
    openrouter_api_key: str = ""
    openrouter_model: str = "openai/gpt-4.1-mini"
    openrouter_site_url: str = ""
    openrouter_app_name: str = "ISMS"

    # --- Firebase Cloud Messaging ---
    firebase_credentials_path: str = "firebase-credentials.json"

    @property
    def allowed_origins_list(self) -> list[str]:
        return [o.strip() for o in self.allowed_origins.split(",")]

    @property
    def normalized_match_approval_mode(self) -> str:
        value = (self.match_approval_mode or "club_admin").strip().lower()
        if value in {"referee_only", "test", "bypass"}:
            return "referee_only"
        return "club_admin"

    @property
    def bypass_club_match_approval(self) -> bool:
        return self.normalized_match_approval_mode == "referee_only"


settings = Settings()  # type: ignore[call-arg]
