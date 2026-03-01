from __future__ import annotations

import os

from dagster import Config
from pydantic import Field, model_validator


class ArcRunConfig(Config):
    pipeline_name: str = "legiscan-arc"
    force_refresh: bool = False
    max_api_calls_per_run: int = 0
    artifact_bucket: str = Field(default_factory=lambda: os.getenv("PIPELINE_ARTIFACT_BUCKET", ""))
    artifact_root_prefix: str = "inference"
    session_state_table: str = Field(default_factory=lambda: os.getenv("PIPELINE_SESSION_STATE_TABLE", "session_state"))
    watermark_table: str = Field(default_factory=lambda: os.getenv("PIPELINE_WATERMARK_TABLE", "pipeline_watermark"))

    @model_validator(mode="after")
    def validate_required_fields(self) -> "ArcRunConfig":
        if not self.artifact_bucket.strip():
            raise ValueError("artifact_bucket must be set")
        return self

