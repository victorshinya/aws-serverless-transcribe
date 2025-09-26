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
        LanguageCode='pt-BR',
        Settings={
            'ShowSpeakerLabels': True,
            'MaxSpeakerLabels': 10  # Adjust based on expected number of speakers
        }
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
                
                # Extract full transcript and speaker segments
                transcript_text = transcript_data['results']['transcripts'][0]['transcript']
                
                # Process speaker segments if available
                speaker_segments = []
                if 'speaker_labels' in transcript_data['results']:
                    # Get all items with speaker labels
                    items = transcript_data['results']['items']
                    segments = transcript_data['results']['speaker_labels']['segments']
                    
                    for segment in segments:
                        # Find words for this speaker segment based on time range
                        segment_words = []
                        start_time = float(segment['start_time'])
                        end_time = float(segment['end_time'])
                        
                        for item in items:
                            if item['type'] == 'pronunciation' and 'start_time' in item:
                                item_start = float(item['start_time'])
                                if start_time <= item_start <= end_time:
                                    if 'alternatives' in item and len(item['alternatives']) > 0:
                                        segment_words.append(item['alternatives'][0]['content'])
                        
                        speaker_segments.append({
                            'speaker': segment['speaker_label'],
                            'start_time': segment['start_time'],
                            'end_time': segment['end_time'],
                            'text': ' '.join(segment_words)
                        })
                
                print(f"Transcript: {transcript_text[:100]}...")
                print(f"Found {len(speaker_segments)} speaker segments")
                
                table = dynamodb.Table('transcriptions')
                table.put_item(
                    Item={
                        'id': str(uuid.uuid4()),
                        'source_bucket': bucket,
                        'source_key': key,
                        'transcript': transcript_text,
                        'speaker_segments': speaker_segments,
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
