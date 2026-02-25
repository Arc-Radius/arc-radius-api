import boto3
import json

# 1. Test connection
print("Testing Bedrock connection...\n")

try:
    client = boto3.client('bedrock-runtime', region_name='us-east-1')
    print("✓ Client created")
except Exception as e:
    print(f"✗ Client failed: {e}")
    exit()

# 2. List available Claude models
try:
    bedrock = boto3.client('bedrock', region_name='us-east-1')
    models = bedrock.list_foundation_models()
    claude_models = [m['modelId']
                     for m in models['modelSummaries'] if 'claude' in m['modelId'].lower()]
    print(f"✓ Available Claude models: {claude_models}")
except Exception as e:
    print(f"✗ Could not list models: {e}")

# 3. Test a simple query
print("\nSending test prompt...")

# change if not available
MODEL_ID = "arn:aws:bedrock:us-east-1:233894721797:inference-profile/global.anthropic.claude-sonnet-4-20250514-v1:0"

try:
    response = client.invoke_model(
        modelId=MODEL_ID,
        body=json.dumps({
            "anthropic_version": "bedrock-2023-05-31",
            "max_tokens": 256,
            "temperature": 0.3,
            "system": "You are a helpful assistant. Respond briefly.",
            "messages": [
                {"role": "user", "content": "What is a legislative bill? One sentence."}
            ],
        }),
    )

    result = json.loads(response['body'].read())
    print(f"✓ Model: {MODEL_ID}")
    print(f"✓ Response: {result['content'][0]['text']}")
    print(
        f"✓ Tokens used — input: {result['usage']['input_tokens']}, output: {result['usage']['output_tokens']}")

except Exception as e:
    print(f"✗ Invoke failed: {e}")
    print("\nTroubleshooting:")
    print("  - Is the model ID correct? Check step 2 output above")
    print("  - Did you enable model access in AWS Console → Bedrock → Model access?")
    print("  - Are your AWS credentials configured? Run: aws configure")
    print("  - Is the region correct? Try us-west-2 if us-east-1 doesn't work")
