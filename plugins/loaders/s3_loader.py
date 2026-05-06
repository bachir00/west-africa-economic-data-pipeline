"""
AWS S3 Data Lake Loader Module

Handles uploading raw and processed data to AWS S3 with:
- Partitioned storage (raw/ vs processed/)
- Multiple format support (CSV, Parquet, JSON)
- Automatic timestamping
- Error handling and logging
"""

import boto3
import pandas as pd
import json
from typing import Dict, Optional
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class S3Loader:
    """Handles data uploads to AWS S3 data lake"""
    
    def __init__(self, bucket_name: str, region_name: str = 'us-east-1', 
                 access_key_id: Optional[str] = None, secret_access_key: Optional[str] = None):
        """
        Initialize S3 Loader
        
        Args:
            bucket_name: S3 bucket name
            region_name: AWS region
            access_key_id: AWS access key (optional, uses default credentials if not provided)
            secret_access_key: AWS secret key (optional)
        """
        self.bucket_name = bucket_name
        self.region_name = region_name
        
        # Initialize S3 client
        session_kwargs = {'region_name': region_name}
        if access_key_id and secret_access_key:
            session_kwargs['aws_access_key_id'] = access_key_id
            session_kwargs['aws_secret_access_key'] = secret_access_key
        
        try:
            self.s3_client = boto3.client('s3', **session_kwargs)
            logger.info(f"✅ S3 Client initialized for bucket: {bucket_name}")
        except Exception as e:
            logger.error(f"❌ Failed to initialize S3 client: {str(e)}")
            raise
    
    def upload_dataframe(self, df: pd.DataFrame, key: str, format: str = 'csv') -> bool:
        """
        Upload DataFrame to S3
        
        Args:
            df: DataFrame to upload
            key: S3 key (file path)
            format: File format ('csv', 'parquet', 'json')
        
        Returns:
            True if successful, False otherwise
        """
        try:
            if format == 'csv':
                data = df.to_csv(index=False)
                content_type = 'text/csv'
            elif format == 'parquet':
                data = df.to_parquet(index=False)
                content_type = 'application/octet-stream'
            elif format == 'json':
                data = df.to_json(orient='records')
                content_type = 'application/json'
            else:
                raise ValueError(f"Unsupported format: {format}")
            
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=key,
                Body=data,
                ContentType=content_type,
                ServerSideEncryption='AES256'
            )
            
            file_size_mb = len(data) / (1024 * 1024)
            logger.info(f"✅ Uploaded {len(df)} records to s3://{self.bucket_name}/{key} ({file_size_mb:.2f} MB)")
            return True
        
        except Exception as e:
            logger.error(f"❌ Error uploading DataFrame to S3: {str(e)}")
            return False
    
    def upload_raw(self, df: pd.DataFrame, format: str = 'csv') -> str:
        """
        Upload raw extracted data to S3 data lake
        
        Args:
            df: Raw DataFrame from extractor
            format: File format (default: csv)
        
        Returns:
            S3 key path
        """
        timestamp = datetime.now().strftime('%Y/%m/%d/%H%M%S')
        key = f"raw/world-bank-api/{timestamp}/data.{format}"
        
        logger.info(f"📤 Uploading raw data to S3: {key}")
        success = self.upload_dataframe(df, key, format=format)
        
        if success:
            return key
        else:
            raise Exception(f"Failed to upload raw data to S3")
    
    def upload_processed(self, df: pd.DataFrame, format: str = 'parquet') -> str:
        """
        Upload processed/transformed data to S3 data lake
        
        Args:
            df: Transformed DataFrame ready for analytics
            format: File format (default: parquet for better compression)
        
        Returns:
            S3 key path
        """
        timestamp = datetime.now().strftime('%Y/%m/%d/%H%M%S')
        key = f"processed/west-africa-economic/{timestamp}/data.{format}"
        
        logger.info(f"📤 Uploading processed data to S3: {key}")
        success = self.upload_dataframe(df, key, format=format)
        
        if success:
            return key
        else:
            raise Exception(f"Failed to upload processed data to S3")
    
    def upload_json(self, data: Dict, key: str) -> bool:
        """
        Upload JSON dictionary to S3
        
        Args:
            data: Dictionary to upload
            key: S3 key
        
        Returns:
            True if successful, False otherwise
        """
        try:
            json_data = json.dumps(data, indent=2, ensure_ascii=False, default=str)
            
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=key,
                Body=json_data.encode('utf-8'),
                ContentType='application/json',
                ServerSideEncryption='AES256'
            )
            
            logger.info(f"✅ Uploaded JSON to s3://{self.bucket_name}/{key}")
            return True
        
        except Exception as e:
            logger.error(f"❌ Error uploading JSON to S3: {str(e)}")
            return False
    
    def upload_files(self, files_dict: Dict[str, pd.DataFrame], 
                    prefix: str = '', format: str = 'csv') -> Dict[str, bool]:
        """
        Upload multiple files to S3
        
        Args:
            files_dict: Dictionary {filename: DataFrame}
            prefix: S3 path prefix
            format: File format
        
        Returns:
            Dictionary with upload results
        """
        results = {}
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        for filename, df in files_dict.items():
            key = f"{prefix}/{timestamp}/{filename}.{format}" if prefix else f"{timestamp}/{filename}.{format}"
            results[filename] = self.upload_dataframe(df, key, format)
        
        return results
    
    def list_objects(self, prefix: str = '') -> list:
        """
        List objects in S3 bucket
        
        Args:
            prefix: Path prefix to filter
        
        Returns:
            List of S3 keys
        """
        try:
            response = self.s3_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=prefix
            )
            
            if 'Contents' in response:
                return [obj['Key'] for obj in response['Contents']]
            return []
        
        except Exception as e:
            logger.error(f"Erreur lors de la lecture du bucket S3: {e}")
            return []
    
    def delete_object(self, key: str) -> bool:
        """
        Supprimer un objet du bucket S3
        
        Args:
            key: Clé S3
        
        Returns:
            True si succès, False sinon
        """
        try:
            self.s3_client.delete_object(
                Bucket=self.bucket_name,
                Key=key
            )
            logger.info(f"Objet supprimé de S3: s3://{self.bucket_name}/{key}")
            return True
        
        except Exception as e:
            logger.error(f"Erreur lors de la suppression de l'objet S3: {e}")
            return False


# Fonction pour être utilisée dans Airflow
def load_data_to_s3(**context):
    """Fonction pour charger les données vers S3 (compatible Airflow)"""
    import os
    
    # Récupérer les données transformées
    task_instance = context['task_instance']
    transformed_data = task_instance.xcom_pull(
        task_ids='transform_economic_data',
        key='transformed_data'
    )
    
    # Configuration S3
    bucket_name = os.getenv('S3_BUCKET_NAME', 'west-africa-economic-data')
    
    # Initialiser le chargeur
    loader = S3Loader(bucket_name)
    
    # Charger les données
    files_to_load = {
        'main_data': transformed_data['main_data'],
        'by_country': transformed_data['by_country'],
        'by_year': transformed_data['by_year'],
        'by_indicator': transformed_data['by_indicator']
    }
    
    results = loader.upload_files(files_to_load, prefix='economic_data', format='csv')
    
    print(f"Données chargées vers S3: {results}")
    return results


if __name__ == "__main__":
    # Test simple
    print("Module de chargement vers S3")
