### Documentation Globale pour l'Ingestion et Traitement de Données

Ce document fournit une vue d'ensemble des processus d'ingestion, de transformation, et de validation de données pour créer une architecture "Médaille" dans Databricks. Le processus est structuré avec des notebooks contenant des étapes spécifiques, et est intégré dans un flux de travail CI/CD avec des tests et validations. 

#### Table des matières

1. **Introduction**
2. **Architecture Médaille**
   - 2.1. Ingestion des Données
   - 2.2. Traitement des Données
   - 2.3. Structuration Médaille
3. **Workflow GitHub et CI/CD**
   - 3.1. Intégration Continue
   - 3.2. Release Sémantique
4. **Validation des Données**
   - 4.1. Tests Unitaires
   - 4.2. Validation avec Great Expectations
5. **Conclusion**

---

### 1. Introduction

Ce document détaille les processus utilisés pour ingérer, traiter, et valider les données à l'aide de Databricks, structuré autour de la méthodologie "Médaille" (Bronze, Silver, Gold).

### 2. Architecture Médaille

#### 2.1. Ingestion des Données

L'ingestion des données se fait en plusieurs étapes :

- **Téléchargement des Fichiers** : Les données sources sont téléchargées sous forme de fichiers `.zip` depuis des liens externes. Ceux-ci sont extraits et uniquement les fichiers `.txt` sont conservés pour être traités dans les dossiers de l'année spécifiée.
- **Paramètres de Stockage** : Les fichiers sont stockés et organisés avec des identifiants uniques pour les distinguer des datasets précédemment ingérés.
- **Assemblage des Données** : Les données brutes sont organisées dans des tables "Bronze" pour un traitement initial.

#### 2.2. Traitement des Données

Le traitement des données implique la transformation des fichiers `.txt` extraits vers des tables spatiales, appelées tables "Silver". Cette étape comprend des nettoyages de base tels que la suppression des doublons et le remplissage des valeurs manquantes. 

#### 2.3. Structuration Médaille

- **Tables Bronze** : Contiennent les données brutes comme initialement reçues.
- **Tables Silver** : Contiennent des données nettoyées et standardisées.
- **Tables Gold** : Représentent les données agrégées et modélisées prêtes pour l'analyse. Les transformations incluent des agrégations statistiques et ajoutent de la précision à travers des jointures et filtres appliqués.

### 3. Workflow GitHub et CI/CD

#### 3.1. Intégration Continue

Les notebooks utilisent des actions GitHub pour s'assurer que chaque morceau de code poussé dans le répertoire est automatiquement testé et vérifié avec une série de checks.

#### 3.2. Release Sémantique

Utilisation de semantic release, un outil automatisé qui génère une version, met à jour le changelog, et publie les releases en fonction des commits.

### 4. Validation des Données

#### 4.1. Tests Unitaires

Tests écrits en utilisant PyTest et conçus pour vérifier l'intégrité de la transformation de données, en assurant que les tables résultantes respectent les règles de validation de base.

#### 4.2. Validation avec Great Expectations

Great Expectations est utilisé pour garantir une validation sur divers points de données et schémas :
- **Intégrité des données** : Assure que les colonnes contiennent des valeurs dans les formats attendus.
- **Contrôles Qualité** : Vérifie que les valeurs agrégées respectent les limites définies.
- **Analytiques** : Validations de cohérence et structure du DataFrame.

### 5. Conclusion

Ce workflow détaillé étend l'usage des outils comme Databricks en conjonction avec des solutions modernes de CI/CD pour garantir une architecture robuste et dimensionnée. Les validations avec des outils standards assurent que les données restent fiables et débarrassées des anomalies avant qu'elles ne soient utilisées pour une analyse ou un apprentissage machine plus poussés.

Cette documentation décrit chaque étape réalisée dans l'agencement des notebooks et l'automatisation de pipeline pour une gestion efficace des données.