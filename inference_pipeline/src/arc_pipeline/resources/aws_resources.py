from __future__ import annotations

from typing import Any

import boto3
from dagster import ConfigurableResource


class AwsClientsResource(ConfigurableResource):
    region_name: str | None = None

    def s3_client(self) -> Any:
        return boto3.client("s3", region_name=self.region_name)

    def dynamodb_client(self) -> Any:
        return boto3.client("dynamodb", region_name=self.region_name)

    def dynamodb_resource(self) -> Any:
        return boto3.resource("dynamodb", region_name=self.region_name)

