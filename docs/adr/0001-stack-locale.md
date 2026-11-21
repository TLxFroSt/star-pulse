# 0001 — Stack locale : Redpanda, MinIO, DuckDB/dbt, Dagster

- Statut : accepté
- Date : 2026-11-21

## Contexte

Le projet doit ingérer des flux temps réel (GTFS-RT), les historiser et calculer des KPI,
le tout sur un laptop, à coût zéro, avec une architecture transposable dans le cloud.

## Décision

- **Redpanda** plutôt qu'Apache Kafka : même API Kafka, un seul binaire sans ZooKeeper/KRaft
  à configurer, empreinte mémoire faible en local. La Console donne une vue des topics.
- **MinIO** comme stockage objet : API S3 compatible, donc le code d'écriture Parquet sera le
  même vers ADLS Gen2 / S3 plus tard. Un bucket unique `lake` avec préfixes
  `bronze/`, `silver/`, `gold/`.
  Les images officielles `minio/minio` et `minio/mc` ne sont plus publiées sur Docker Hub ;
  on utilise le fork communautaire `pgsty/minio` / `pgsty/mc` (Pigsty), même binaire et même
  API, maintenu à jour. Tags figés dans `docker-compose.yml`.
- **DuckDB + dbt-duckdb** pour les transformations : SQL analytique rapide sur Parquet sans
  cluster ; les modèles dbt restent portables vers Databricks / Spark.
- **Dagster** pour l'orchestration : modèle par *assets* aligné sur les couches du lakehouse,
  asset checks natifs pour la qualité.
- **uv** pour les dépendances Python : résolution rapide et `uv.lock` reproductible.

## Conséquences

- `make up` suffit pour avoir toute l'infra ; aucun service payant.
- Le passage au cloud (bonus Terraform Azure) se limite à changer endpoints et identifiants
  dans la configuration.
