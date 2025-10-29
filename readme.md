# Documentation du Projet de Qualité de l'Eau en France

## Présentation du Projet

Ce projet vise à analyser et suivre la qualité de l'eau potable dans les communes françaises. Il s'appuie sur des données publiques fournies par le ministère de la Santé et les agences régionales de santé, disponibles sur [data.gouv.fr](https://www.data.gouv.fr).

### Contexte
- **Domaine** : Qualité de l'eau potable
- **Source des données** : Données annuelles de contrôle sanitaire (2016-2025) téléchargées depuis data.gouv.fr
- **Technologies** : PySpark, Databricks, Azure Data Lake Storage, Great Expectations
- **Architecture** : Pipeline de données en couches (Bronze → Silver → Gold)

## Architecture Technique

### Structure du Projet

```bash
.github/workflows/
├── ci.yaml                # CI/CD pour les tests
├── release.yaml           # Gestion des releases
notebooks/
├── 01_SOURCE_TO_LANDZONE.ipynb  # Téléchargement et stockage initial
├── 02_LANDZONE_TO_BRONZE.ipynb  # Chargement en tables Bronze
├── 03_BRONZE_TO_SILVER.ipynb    # Nettoyage et enrichissement
├── 04_SILVER_TO_GOLD.ipynb      # Agrégations analytiques
├── 05_DATA_VALIDATION.ipynb     # Validation des données
scripts/
├── 01_SOURCE_TO_LANDZONE.py
├── 02_LANDZONE_TO_BRONZE.py
├── 03_BRONZE_TO_SILVER.py
├── 04_SILVER_TO_GOLD.py
├── 05_DATA_VALIDATION.py
tests/
├── test_transformations.py      # Tests unitaires basée sur Databricks et Azure. L'intégration Git permet une gestion efficace du code, tandis que les pipelines automatisés 

## Pipeline de Données

1. **Source → Landzone** :
   - Téléchargement des fichiers ZIP depuis data.gouv.fr
   - Extraction des fichiers texte
   - Stockage dans Azure Data Lake Storage (container "source")

2. **Landzone → Bronze** :
   - Chargement des fichiers CSV bruts
   - Création des tables Delta Lake (bronze_plv, bronze_com, bronze_result)

3. **Bronze → Silver** :
   - Nettoyage des données (standardisation des codes, formats de dates)
   - Enrichissement avec des jointures
   - Création des tables Delta Lake (silver_com, silver_plv, silver_result)

4. **Silver → Gold** :
   - Agrégations analytiques
   - Calculs de conformité
   - Création des tables Delta Lake (gold_sample_summary, gold_param_stats, etc.)

5. **Validation** :
   - Vérification de la qualité des données avec Great Expectations
   - Validation des formats, valeurs attendues, etc.

## Intégration avec Databricks

### Fonctionnalités Utilisées

- **Pipelines Databricks** : Les notebooks servent de base aux pipelines de données configurés dans Databricks
- **Suivi Git** : Intégration complète avec GitHub :
  - Suivi des branches directement depuis l'interface Databricks
  - Visualisation des commits et historique
- **Secrets Management** : Utilisation des secrets Databricks pour les clés d'accès Azure
- **Delta Lake** : Stockage des données dans des tables Delta optimisées

### Configuration Azure

- **Data Lake Storage** : Stockage intermédiaire des fichiers bruts
- **Authentification** : Utilisation de clés d'accès sécurisées via Databricks Secrets
- **Conteneurs** :
  - `source` : Fichiers bruts téléchargés

## CI/CD

### Workflow CI (ci.yaml)

- **Déclenchement** : Sur push vers `main` 
- **Étapes** :
  1. Checkout du code
  2. Installation de uv (gestionnaire de dépendances Python)
  3. Installation du projet
  4. Exécution des tests pytest

### Workflow Release (release.yaml)

- **Déclenchement** : Sur push vers `main`
- **Permissions** : Écriture sur le dépôt
- **Étapes** :
  1. Checkout du code
  2. Configuration de Python et uv
  3. Exécution de semantic-release pour la gestion des versions

## Tests

Le fichier `test_transformations.py` contient des tests unitaires pour les fonctions de nettoyage :
- `clean_plv()` : Nettoyage des données de prélèvement
- `clean_commune()` : Nettoyage des données de communes
- `clean_result()` : Nettoyage des résultats d'analyse


assurent une mise à jour régulière des données.