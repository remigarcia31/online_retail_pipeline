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

from include.dbt.cosmos_config import DBT_PROJECT_CONFIG, DBT_CONFIG
from cosmos.airflow.task_group import DbtTaskGroup
from cosmos.constants import LoadMode
from cosmos.config import ProjectConfig, RenderConfig

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
            name='raw_transactions',  # Nom de la table BigQuery cible
            conn_id='gcp',  # Connexion à Google Cloud Platform
            metadata=Metadata(schema='retail')  # Schéma BigQuery dans lequel créer la table
        ),
        use_native_support=False,  # Désactive l'utilisation de fonctions natives pour un contrôle précis
    )

    # Définition de la séquence d'exécution des tâches
    # upload_csv_to_gcs >> create_retail_dataset >> gcs_to_raw
    
    @task.external_python(python='/usr/local/airflow/soda_venv/bin/python')
    def check_load(scan_name='check_load', checks_subpath='sources'):
        """
        Tâche Airflow pour exécuter un scan Soda en utilisant un environnement Python virtuel.

        Args:
            scan_name (str, optional): Nom unique pour le scan Soda. Défaut : 'check_load'.
            checks_subpath (str, optional): Sous-chemin pour les fichiers de vérification Soda. Défaut : 'sources'.

        Returns:
            int: Résultat du scan Soda (0 si réussi, autre chose en cas d'échec).
        """
        # Importation de la fonction `check` définie dans le répertoire `include/soda`
        from include.soda.check_function import check

        # Exécution de la fonction `check` avec les paramètres fournis
        return check(scan_name, checks_subpath)

    # Appel de la tâche pour l'exécuter dans le DAG
    check_load()

    # Transformation des données avec dbt
    transform = DbtTaskGroup(
        group_id='transform',  # Groupe de tâches dbt
        project_config=DBT_PROJECT_CONFIG,  # Configuration dbt (projet)
        profile_config=DBT_CONFIG,  # Configuration dbt (profil)
        render_config=RenderConfig(  # Configuration de rendu des transformations
            load_method=LoadMode.DBT_LS,  # Méthode de chargement basée sur `dbt ls`
            select=['path:models/transform']  # Modèles dbt sélectionnés pour cette étape
        )
    )

    @task.external_python(python='/usr/local/airflow/soda_venv/bin/python')  
    def check_transform(scan_name='check_transform', checks_subpath='transform'):
        """
        Tâche Airflow utilisant un environnement Python externe pour exécuter un scan Soda.

        Args:
            scan_name (str, optional): Nom unique pour le scan Soda. Défaut : 'check_transform'.
            checks_subpath (str, optional): Sous-chemin pour les fichiers de vérification Soda. Défaut : 'transform'.

        Returns:
            int: Résultat du scan Soda (0 si réussi, autre chose en cas d'échec).
        """
        # Importation de la fonction `check` définie dans le répertoire `include/soda`
        from include.soda.check_function import check

        # Exécution de la fonction `check` avec les paramètres fournis
        return check(scan_name, checks_subpath)

    # Appel explicite de la tâche pour qu'elle soit incluse dans le DAG
    check_transform()

    # Création d'un groupe de tâches DbtTaskGroup pour les rapports
    report = DbtTaskGroup(
        group_id='report',  # Identifiant unique du groupe de tâches
        project_config=DBT_PROJECT_CONFIG,  # Configuration du projet dbt
        profile_config=DBT_CONFIG,  # Configuration du profil dbt
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,  # Méthode de chargement pour dbt (DBT_LS indique le chargement depuis le système de fichiers)
            select=['path:models/report']  # Sélection spécifique des modèles à inclure dans le rendu
        )
    )

    report()  # Appel de la tâche pour l'exécuter dans le DAG


retail()
