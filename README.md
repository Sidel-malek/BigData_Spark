# Utilisations de Spark

Ce dépôt rassemble diverses utilisations des fonctionnalités d'Apache Spark : configuration en mode standalone, utilisation de Spark SQL pour des requêtes de données, et machine learning avec Spark MLlib.

## Projets

1. **Spark_StandAlone**
   - **Description** : Installation et configuration d'un cluster Spark en mode Standalone avec Docker.
   - **Contenu** : les données `arbre.csv` , script PySpark (`spark_job.py`) pour traiter des données en CSV.
   
2. **SparkSQL**
   - **Description** : Utilisation de Spark SQL pour effectuer des requêtes sur de grandes bases de données.
   - **Contenu** : Script PySpark pour exécuter des requêtes SQL sur des datasets Ngram.
     `Google NGram Dataset : http://storage.googleapis.com/books/ngrams/books/datasetsv2.html`

3. **SparkMLlib**
   - **Description** : Projet de machine learning utilisant Spark MLlib.
   - **Contenu** : Script PySpark pour entraîner un modèle ML simple (par exemple, classification ou régression) sur un dataset.

## Prérequis
- **Docker** (pour Spark_StandAlone)
- **Apache Spark** et **PySpark**
