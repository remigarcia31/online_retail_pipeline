from airflow.decorators import dag, task
from datetime import datetime

# Importation de l'opérateur pour transférer un fichier local vers Google Cloud Storage (GCS)
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
# Importation de l'opérateur pour créer un dataset vide dans BigQuery
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator

# Importation des modules d'Astro SDK pour manipuler des fichiers et des tables SQL
from astro import sql as aql
from astro.files import File
from astro.sql.table import Table, Metadata
from astro.constants import FileType

# Définition du DAG avec ses paramètres principaux
@dag(
    start_date=datetime(2025, 1, 1),  # Date de début d'exécution du DAG
    schedule=None,  # Ce DAG ne sera pas exécuté périodiquement (pas de planification)
    catchup=False,  # Désactive la rétro-exécution des exécutions manquées
    tags=['retail'],  # Ajout d'un tag pour catégoriser le DAG
)
def retail():
    """
    DAG pour transférer un fichier CSV local contenant des données de ventes au détail 
    vers un bucket Google Cloud Storage (GCS), créer un dataset BigQuery vide, 
    et charger les données du fichier dans une table brute.
    """

    # Tâche pour uploader un fichier CSV vers un bucket GCS
    upload_csv_to_gcs = LocalFilesystemToGCSOperator(
        task_id='upload_csv_to_gcs',  # Identifiant unique de la tâche dans le DAG
        src='include/dataset/retail_sales_dataset.csv',  # Chemin local du fichier source
        dst='raw/online_retail.csv',  # Chemin de destination dans le bucket GCS
        bucket='online_retail_sales_bucket',  # Nom du bucket GCS cible
        gcp_conn_id='gcp',  # Identifiant de connexion à Google Cloud Platform
        mime_type='text/csv',  # Type MIME du fichier à uploader
    )

    # Tâche pour créer un dataset vide dans BigQuery
    create_retail_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='create_retail_dataset',  # Identifiant unique de la tâche dans le DAG
        dataset_id='retail',  # Identifiant du dataset BigQuery à créer
        gcp_conn_id='gcp',  # Identifiant de connexion à Google Cloud Platform
    )

    # Tâche pour charger les données du fichier GCS dans une table BigQuery brute
    gcs_to_raw = aql.load_file(
        task_id='gcs_to_raw',  # Identifiant unique de la tâche dans le DAG
        input_file=File(
            'gs://online_retail_sales_bucket/raw/online_retail.csv',  # Chemin du fichier dans GCS
            conn_id='gcp',  # Connexion à Google Cloud Platform
            filetype=FileType.CSV,  # Type du fichier (CSV)
        ),
        output_table=Table(
            name='raw_invoices',  # Nom de la table BigQuery cible
            conn_id='gcp',  # Connexion à Google Cloud Platform
            metadata=Metadata(schema='retail')  # Schéma BigQuery dans lequel créer la table
        ),
        use_native_support=False,  # Désactive l'utilisation de fonctions natives pour un contrôle précis
    )

    # Définition de la séquence d'exécution des tâches
    upload_csv_to_gcs >> create_retail_dataset >> gcs_to_raw

retail()
