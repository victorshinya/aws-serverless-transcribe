import json
import boto3
import uuid
import time
from datetime import datetime
import urllib.request

s3 = boto3.client('s3')
transcribe = boto3.client('transcribe')
dynamodb = boto3.resource('dynamodb')

def lambda_handler(event, context):
    print(f"Received event: {json.dumps(event)}")
    
    # Handle S3 event trigger
    if 'Records' in event:
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = event['Records'][0]['s3']['object']['key']
        print(f"Processing file: s3://{bucket}/{key}")
    else:
        # Handle direct invocation
        bucket = event['bucket']
        key = event['key']
        print(f"Direct invocation: s3://{bucket}/{key}")
    
    job_name = f"transcribe-{uuid.uuid4()}"
    print(f"Starting transcription job: {job_name}")
    
    transcribe.start_transcription_job(
        TranscriptionJobName=job_name,
        Media={'MediaFileUri': f's3://{bucket}/{key}'},
        MediaFormat='mp3',
        LanguageCode='pt-BR'
    )
    
    # Poll with exponential backoff
    wait_time = 10
    max_wait = 300
    
    while wait_time <= max_wait:
        time.sleep(wait_time)
        
        try:
            response = transcribe.get_transcription_job(TranscriptionJobName=job_name)
            status = response['TranscriptionJob']['TranscriptionJobStatus']
            print(f"Transcription status: {status}")
            
            if status == 'COMPLETED':
                transcript_uri = response['TranscriptionJob']['Transcript']['TranscriptFileUri']
                print(f"Transcript URI: {transcript_uri}")
                
                # Use urllib to fetch the transcript from the public URL
                with urllib.request.urlopen(transcript_uri) as response:
                    transcript_data = json.loads(response.read().decode())
                
                transcript_text = transcript_data['results']['transcripts'][0]['transcript']
                print(f"Transcript: {transcript_text[:100]}...")
                
                table = dynamodb.Table('transcriptions')
                table.put_item(
                    Item={
                        'id': str(uuid.uuid4()),
                        'source_bucket': bucket,
                        'source_key': key,
                        'transcript': transcript_text,
                        'timestamp': datetime.now().isoformat(),
                        'language': 'pt-BR'
                    }
                )
                print("Saved to DynamoDB")
                
                transcribe.delete_transcription_job(TranscriptionJobName=job_name)
                
                return {
                    'statusCode': 200,
                    'body': json.dumps({
                        'message': 'Transcription completed',
                        'transcript': transcript_text
                    })
                }
                
            elif status == 'FAILED':
                print("Transcription failed")
                return {
                    'statusCode': 500,
                    'body': json.dumps({'error': 'Transcription failed'})
                }
            
            # Exponential backoff
            wait_time = min(wait_time * 1.5, 60)
            
        except Exception as e:
            print(f"Error during polling: {str(e)}")
            if 'ThrottlingException' in str(e):
                wait_time = min(wait_time * 2, 120)
                continue
            raise e
    
    print("Transcription timeout")
    return {
        'statusCode': 408,
        'body': json.dumps({'error': 'Transcription timeout'})
    }
