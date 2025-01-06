def check(scan_name, checks_subpath=None, data_source='retail', project_root='include'):
    """
    Fonction pour exécuter un scan Soda SQL sur une source de données spécifique.
    
    Args:
        scan_name (str): Nom unique du scan (utilisé pour le suivi et les logs).
        checks_subpath (str, optional): Sous-chemin vers un fichier ou dossier de vérifications spécifiques.
        data_source (str, optional): Nom de la source de données à analyser (par défaut : 'retail').
        project_root (str, optional): Répertoire racine où se trouvent les fichiers Soda (par défaut : 'include').
    
    Returns:
        int: Résultat du scan (0 si réussi, autre chose en cas d'échec).
    
    Raises:
        ValueError: Si le scan échoue (résultat différent de 0).
    """
    from soda.scan import Scan  # Importation de la classe Scan de la bibliothèque Soda SQL

    print('Running Soda Scan ...')  # Message informatif pour indiquer le démarrage du scan

    # Chemin du fichier de configuration Soda
    config_file = f'{project_root}/soda/configuration.yml'
    # Chemin du répertoire contenant les fichiers SodaCL de vérification
    checks_path = f'{project_root}/soda/checks'

    # Si un sous-chemin spécifique est fourni, l'ajouter au chemin de base
    if checks_subpath:
        checks_path += f'/{checks_subpath}'

    # Initialisation d'un objet Scan de Soda
    scan = Scan()

    # Activation des logs détaillés pour faciliter le débogage
    scan.set_verbose()

    # Ajout du fichier de configuration Soda à l'objet Scan
    scan.add_configuration_yaml_file(config_file)

    # Définition de la source de données à analyser
    scan.set_data_source_name(data_source)

    # Ajout des fichiers SodaCL (langage de configuration Soda) pour les vérifications
    scan.add_sodacl_yaml_files(checks_path)

    # Définition du nom unique du scan (utilisé pour les résultats et les logs)
    scan.set_scan_definition_name(scan_name)

    # Exécution du scan
    result = scan.execute()

    # Affichage des logs générés pendant l'exécution du scan
    print(scan.get_logs_text())

    # Si le scan échoue (résultat différent de 0), lever une exception
    if result != 0:
        raise ValueError('Soda Scan failed')

    # Retourner le résultat du scan
    return result
