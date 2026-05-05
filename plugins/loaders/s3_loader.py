"""
Module de chargement des données vers AWS S3
"""

import boto3
import pandas as pd
import json
from typing import Dict, Optional
from datetime import datetime
import logging


logger = logging.getLogger(__name__)


class S3Loader:
    """Classe pour charger les données vers AWS S3"""
    
    def __init__(self, bucket_name: str, region_name: str = 'us-east-1', 
                 access_key_id: Optional[str] = None, secret_access_key: Optional[str] = None):
        """
        Initialiser le chargeur S3
        
        Args:
            bucket_name: Nom du bucket S3
            region_name: Région AWS
            access_key_id: Clé d'accès AWS (optionnel, utilise les credentials par défaut)
            secret_access_key: Clé secrète AWS (optionnel)
        """
        self.bucket_name = bucket_name
        self.region_name = region_name
        
        # Initialiser le client S3
        session_kwargs = {'region_name': region_name}
        if access_key_id and secret_access_key:
            session_kwargs['aws_access_key_id'] = access_key_id
            session_kwargs['aws_secret_access_key'] = secret_access_key
        
        self.s3_client = boto3.client('s3', **session_kwargs)
    
    def upload_dataframe(self, df: pd.DataFrame, key: str, format: str = 'csv') -> bool:
        """
        Charger un DataFrame vers S3
        
        Args:
            df: DataFrame à charger
            key: Clé S3 (chemin du fichier)
            format: Format du fichier ('csv', 'parquet', 'json')
        
        Returns:
            True si succès, False sinon
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
                raise ValueError(f"Format non supporté: {format}")
            
            self.s3_client.put_object(
                Bucket=self.bucket_name,
                Key=key,
                Body=data,
                ContentType=content_type,
                ServerSideEncryption='AES256'
            )
            
            logger.info(f"DataFrame chargé vers S3: s3://{self.bucket_name}/{key}")
            return True
        
        except Exception as e:
            logger.error(f"Erreur lors du chargement du DataFrame vers S3: {e}")
            return False
    
    def upload_json(self, data: Dict, key: str) -> bool:
        """
        Charger un dictionnaire JSON vers S3
        
        Args:
            data: Dictionnaire à charger
            key: Clé S3
        
        Returns:
            True si succès, False sinon
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
            
            logger.info(f"JSON chargé vers S3: s3://{self.bucket_name}/{key}")
            return True
        
        except Exception as e:
            logger.error(f"Erreur lors du chargement du JSON vers S3: {e}")
            return False
    
    def upload_files(self, files_dict: Dict[str, pd.DataFrame], 
                    prefix: str = '', format: str = 'csv') -> Dict[str, bool]:
        """
        Charger plusieurs fichiers vers S3
        
        Args:
            files_dict: Dictionnaire {nom_fichier: DataFrame}
            prefix: Préfixe du chemin S3
            format: Format des fichiers
        
        Returns:
            Dictionnaire avec les résultats de chargement
        """
        results = {}
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        
        for filename, df in files_dict.items():
            key = f"{prefix}/{timestamp}/{filename}.{format}" if prefix else f"{timestamp}/{filename}.{format}"
            results[filename] = self.upload_dataframe(df, key, format)
        
        return results
    
    def list_objects(self, prefix: str = '') -> list:
        """
        Lister les objets dans le bucket S3
        
        Args:
            prefix: Préfixe du chemin
        
        Returns:
            Liste des clés d'objets
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
