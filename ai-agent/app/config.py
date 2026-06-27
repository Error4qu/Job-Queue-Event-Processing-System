from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    app_name: str = "job-queue-ai-agent"
    host: str = "0.0.0.0"
    port: int = 8090

    job_queue_base_url: str = "http://app:8080"
    admin_api_key: str = "change-me-in-production"

    kafka_bootstrap_servers: str = "kafka:29092"
    incident_topic: str = "incident-topic"
    kafka_group_id: str = "ai-agent-group"

    self_healer_bin: str = "/usr/local/bin/self-healer"
    self_healer_enabled: bool = True

    openai_api_key: str = ""
    openai_model: str = "gpt-4o-mini"

    auto_execute: bool = True
    min_confidence: float = 0.75
    dry_run: bool = False

    dedup_ttl_seconds: int = 300


settings = Settings()
