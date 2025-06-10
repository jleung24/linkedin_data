import boto3
import gzip
import io
import logging


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    handlers=[
        logging.StreamHandler(),  # Log to console
        logging.FileHandler('s3_client.log')
    ]
)

def upload_html(html_data: str, object_key: str, bucket_name: str):
    s3_client = boto3.client("s3")
    compressed_data = compress_html(html_data)
    try:
        s3_client.upload_fileobj(
            compressed_data,
            bucket_name,
            object_key,
            ExtraArgs={
                'ContentType': 'text/html',
                'ContentEncoding': 'gzip'
            }
        )
        logging.info(f"Uploaded to '{bucket_name}/{object_key}' successfully.")
    except Exception as e:
        logging.error(f"An error occurred: {e}")
    
def compress_html(html_data: str):
    compressed_buffer = io.BytesIO()
    with gzip.GzipFile(fileobj=compressed_buffer, mode='wb') as gz:
        gz.write(html_data.encode('utf-8'))
    compressed_buffer.seek(0)

    return compressed_buffer

def get_html(bucket_name: str, date: str) -> dict:
    s3_client = boto3.client("s3")
    paginator = s3_client.get_paginator('list_objects_v2')
    prefix = f"{date}/"
    html_data = {}

    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        for obj in page.get('Contents', []):
            key = obj['Key']

            response = s3_client.get_object(Bucket=bucket_name, Key=key)
            with gzip.GzipFile(fileobj=io.BytesIO(response['Body'].read())) as gz:
                html_str = gz.read().decode('utf-8')
                html_data[key] = html_str

    return html_data
