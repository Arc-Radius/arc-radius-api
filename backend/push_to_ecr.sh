#!/bin/bash
# Arc Radius API — Build and Push to ECR

# Configuration
AWS_REGION="us-east-1"
AWS_ACCOUNT_ID="233894721797"
REPO_NAME="arc-radius-api"

# Colors
GREEN='\033[0;32m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${BLUE}=== Arc Radius API — Build & Push to ECR ===${NC}\n"

# Step 1: Git hash for tag
GIT_HASH=$(git rev-parse --short HEAD)
if [ -z "$GIT_HASH" ]; then
    echo -e "${RED}Error: Could not get git commit hash.${NC}"
    exit 1
fi
echo -e "${GREEN}✓ Git commit hash: ${GIT_HASH}${NC}"

# Step 2: Image URIs
ECR_REPO="${AWS_ACCOUNT_ID}.dkr.ecr.${AWS_REGION}.amazonaws.com"
FULL_IMAGE="${ECR_REPO}/${REPO_NAME}"
IMAGE_TAG="${FULL_IMAGE}:${GIT_HASH}"
LATEST_TAG="${FULL_IMAGE}:latest"

echo -e "${GREEN}✓ Image: ${IMAGE_TAG}${NC}\n"

# Step 3: Authenticate
echo -e "${BLUE}Authenticating Docker to ECR...${NC}"
aws ecr get-login-password --region ${AWS_REGION} | \
    docker login --username AWS --password-stdin ${ECR_REPO}

if [ $? -ne 0 ]; then
    echo -e "${RED}Error: Failed to authenticate with ECR${NC}"
    exit 1
fi
echo -e "${GREEN}✓ Authenticated${NC}\n"

# Step 4: Build (linux/amd64 for Lambda)
echo -e "${BLUE}Building Docker image for linux/amd64...${NC}"
docker buildx build --platform linux/amd64 -t ${REPO_NAME}:${GIT_HASH} .

if [ $? -ne 0 ]; then
    echo -e "${RED}Error: Docker build failed${NC}"
    exit 1
fi
echo -e "${GREEN}✓ Built successfully${NC}\n"

# Step 5: Tag (both git hash and latest)
echo -e "${BLUE}Tagging...${NC}"
docker tag ${REPO_NAME}:${GIT_HASH} ${IMAGE_TAG}
docker tag ${REPO_NAME}:${GIT_HASH} ${LATEST_TAG}
echo -e "${GREEN}✓ Tagged: ${GIT_HASH} + latest${NC}\n"

# Step 6: Push
echo -e "${BLUE}Pushing to ECR...${NC}"
docker push ${IMAGE_TAG}
docker push ${LATEST_TAG}

if [ $? -ne 0 ]; then
    echo -e "${RED}Error: Failed to push${NC}"
    exit 1
fi
echo -e "${GREEN}✓ Pushed to ECR${NC}\n"

# Step 7: Update Lambda function
echo -e "${BLUE}Updating Lambda function...${NC}"
aws lambda update-function-code \
    --function-name arc-radius-api \
    --image-uri ${IMAGE_TAG} \
    --region ${AWS_REGION} 2>/dev/null

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ Lambda updated to ${GIT_HASH}${NC}\n"
else
    echo -e "${BLUE}ℹ Lambda function 'arc-radius-api' not found — create it in console first${NC}\n"
fi

# Summary
echo -e "${BLUE}=== Summary ===${NC}"
echo -e "Repository:  ${FULL_IMAGE}"
echo -e "Tag:         ${GIT_HASH}"
echo -e "Latest:      ${LATEST_TAG}"
echo -e "Platform:    linux/amd64"
echo -e ""
echo -e "${GREEN}Next steps:${NC}"
echo -e "  1. Create Lambda function (container image) if not done"
echo -e "  2. Create HTTP API Gateway with ANY /{proxy+} route"
echo -e "  3. Test: curl https://YOUR_API_ID.execute-api.us-east-1.amazonaws.com/docs"
