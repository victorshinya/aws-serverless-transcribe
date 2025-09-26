#!/bin/bash

# Variables
STACK_NAME="transcribe-mp3-stack"
REGION="sa-east-1"
PROFILE="ag"

echo "Building SAM application..."
sam build

echo "Deploying SAM application..."
sam deploy \
    --stack-name $STACK_NAME \
    --capabilities CAPABILITY_IAM \
    --region $REGION \
    --profile $PROFILE \
    --resolve-s3 \
    --no-confirm-changeset

echo "Deployment complete!"
echo "Upload MP3 files to the S3 bucket to test transcription."
