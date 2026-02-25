# bedrock_client.py
import boto3
import json
from functools import lru_cache


@lru_cache()  # singleton — reuse the client
def get_bedrock_client(region: str = 'us-east-1'):
    return boto3.client('bedrock-runtime', region_name=region)


def generate(
    prompt: str,
    system: str = "",
    model_id: str = "arn:aws:bedrock:us-east-1:233894721797:inference-profile/global.anthropic.claude-sonnet-4-20250514-v1:0",
    max_tokens: int = 1024,
    temperature: float = 0.3,
):
    """
    Uses the Messages API format (current standard for Claude on Bedrock).
    NOT the old completion API with 'Human:/Assistant:' prompts.
    """
    client = get_bedrock_client()

    body = {
        "anthropic_version": "bedrock-2023-05-31",  # required for Messages API
        "max_tokens": max_tokens,
        "temperature": temperature,
        "messages": [
            {"role": "user", "content": prompt}
        ],
    }

    # System prompt goes at top level, not inside messages
    if system:
        body["system"] = system

    response = client.invoke_model(
        modelId=model_id,
        body=json.dumps(body),
    )

    result = json.loads(response['body'].read())
    return result['content'][0]['text']
