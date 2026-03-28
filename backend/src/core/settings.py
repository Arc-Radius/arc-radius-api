import json
import os

from dotenv import load_dotenv

load_dotenv()


def _parse_cors_origins(raw_value: str | None) -> list[str]:
    if not raw_value:
        return [
            "http://localhost:8081",
            "http://localhost:19006",
            "http://127.0.0.1:8081",
            "http://127.0.0.1:19006",
        ]

    raw_value = raw_value.strip()
    if raw_value.startswith("["):
        try:
            parsed = json.loads(raw_value)
            if isinstance(parsed, list):
                return [str(origin).strip() for origin in parsed if str(origin).strip()]
        except json.JSONDecodeError:
            pass

    return [origin.strip() for origin in raw_value.split(",") if origin.strip()]


class Settings:
    @property
    def cors_origins(self) -> list[str]:
        return _parse_cors_origins(os.getenv("BACKEND_CORS_ORIGINS"))

    @property
    def neo4j_uri(self) -> str | None:
        raw = os.getenv("NEO4J_URI")
        return raw.strip() if raw and raw.strip() else None

    @property
    def neo4j_user(self) -> str | None:
        raw = os.getenv("NEO4J_USER")
        return raw.strip() if raw and raw.strip() else None

    @property
    def neo4j_password(self) -> str:
        return os.getenv("NEO4J_PASSWORD", "")

    @property
    def neo4j_ui_enabled(self) -> bool:
        """Opt-in: UI list/detail states+bills read from Neo4j (separate from RAG graph env)."""
        flag = os.getenv("NEO4J_UI_ENABLED", "").strip().lower()
        if flag not in ("1", "true", "yes"):
            return False
        return bool(self.neo4j_uri and self.neo4j_user)

    @property
    def rag_and_generation_enabled(self) -> bool:
        """
        When false, GET /bills/rag returns 404 and the /generate router is not mounted.
        Set RAG_AND_GENERATION_ENABLED=false (or 0, no, off) in Lambda/prod to disable.
        Unset or true keeps current default behavior for local dev.
        """
        raw = os.getenv("RAG_AND_GENERATION_ENABLED", "true").strip().lower()
        return raw not in ("0", "false", "no", "off")


settings = Settings()
