from cosmos.config import ProfileConfig, ProjectConfig  # Importation des classes de configuration de Cosmos
from pathlib import Path  # Importation pour gérer les chemins de fichiers

# Configuration du profil dbt
DBT_CONFIG = ProfileConfig(
    profile_name='retail',  # Nom du profil dbt utilisé (doit correspondre à un profil dans le fichier profiles.yml)
    target_name='dev',  # Cible active dans ce profil (ex. : dev, prod)
    profiles_yml_filepath=Path('/usr/local/airflow/include/dbt/profiles.yml')  # Chemin absolu vers le fichier profiles.yml
)

# Configuration du projet dbt
DBT_PROJECT_CONFIG = ProjectConfig(
    dbt_project_path='/usr/local/airflow/include/dbt/',  # Chemin absolu vers le répertoire contenant le projet dbt
)
