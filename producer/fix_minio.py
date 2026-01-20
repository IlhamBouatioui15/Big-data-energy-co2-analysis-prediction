import io
import json
import time

def backup_to_minio_fixed(minio_client, bucket, data_type, data):
    
    try:
        timestamp = int(time.time())
        file_name = f"{data_type}/energy_{data_type}_{timestamp}.json"
        
        data_json = json.dumps(data, indent=2)
        data_bytes = data_json.encode('utf-8')
        data_stream = io.BytesIO(data_bytes)
        
        minio_client.put_object(
            bucket,
            file_name,
            data_stream,
            len(data_bytes),
            content_type='application/json'
        )
        print(f" Backup MinIO: {file_name}")
        return True
    except Exception as e:
        print(f" Erreur backup MinIO: {e}")
        return False
