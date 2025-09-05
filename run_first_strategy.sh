#!/bin/bash
set -e  # Exit immediately if a command fails
set -x  # Print each command before executing

export PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

# Get absolute path of current script location (project root)
DIR="$(cd "$(dirname "$0")" && pwd)"

# Image and container names
IMAGE_NAME="first-strategy-app"
CONTAINER_NAME="first-strategy-app"

#access token 
# docker run --rm \
#   -v "$DIR":/app \
#   -w /app \
#   python:3.12-slim \
#   /bin/sh -c "pip install requests && python access_token.py"


# Step 1: Remove old container (if exists)
docker rm -f "$CONTAINER_NAME" 2>/dev/null || true

# # Step 2: Remove old image (optional)
docker rmi -f "$IMAGE_NAME" 2>/dev/null || true

# # Step 3: Build Docker image (relative Dockerfile path)
docker build -t "$IMAGE_NAME" -f "$DIR/Dockerfile" "$DIR"

# Step 4: Run container with local volume mount
docker run --rm \
  --name "$CONTAINER_NAME" \
  -v "$DIR":/first-strategy \
  -e TZ=Asia/Kolkata \
  "$IMAGE_NAME"

