# 0007 — Orchestration Dagster

- Statut : accepté
- Date : 2026-11-21

## Contexte

Le pipeline a deux rythmes : l'ingestion du GTFS théorique (une fois par jour) et la
transformation silver / gold (dès que du temps réel arrive). Le temps réel lui-même est
un flux continu (producteur → Redpanda → consumer). Il faut planifier, déclencher sur
événement, voir le lignage de bout en bout et remonter la qualité des données.

## Décision

- **Dagster, en software-defined assets** : on décrit les données produites, pas des
  tâches. `dagster-dbt` transforme chaque modèle dbt en asset et chaque test dbt en
  *asset check* (sévérité dbt conservée), sans dupliquer les tests.
- **Bronze** :
  - `bronze/gtfs_static/<table>` : un `multi_asset` qui appelle l'ingestion de
    l'étape 1 (idempotente) ; une clé par table pour que chaque source dbt ait son
    amont exact dans le lignage ;
  - `bronze/gtfs_rt/<flux>` : assets **externes**. Le streaming reste hors Dagster
    (`make produce-rt` / `make consume-rt`) : un orchestrateur batch n'est pas fait
    pour des process qui tournent en continu. Dagster mesure leur fraîcheur.
- **Planification** :
  - `gtfs_static_daily` : 4 h (Europe/Paris), après la republication STAR de 23 h 50 ;
  - `transform_every_15_min` : fraîcheur du temps réel puis `dbt build`.
- **Sensor** `new_gtfs_version_sensor` : relance silver + gold dès qu'une *nouvelle*
  version GTFS est ingérée (métadonnée `status = ingested`), rien si inchangée.
- **Asset checks Python** (en plus des tests dbt) : fraîcheur du bronze temps réel
  (< 10 min, avertissement) et volume minimal du GTFS (erreur si archive tronquée).
  Les règles sont des fonctions pures (`orchestration/checks.py`), testées sans Dagster.
- **Une seule exécution dbt à la fois** (`tag_concurrency_limits` dans
  `orchestration/dagster.yaml`) : DuckDB n'accepte qu'un écrivain.
- **Instance locale** : `DAGSTER_HOME=orchestration/`, historique ignoré par Git.

## Conséquences

- `make dagster` suffit pour tout planifier ; schedules et sensor sont actifs par défaut.
- Les deux tests dbt singuliers (part de passages rapprochés, cohérence des KPI)
  portent sur plusieurs modèles : Dagster les remonte en observations, pas en checks.
- `dagster-webserver` est une dépendance de développement (interface locale) ; un
  déploiement utiliserait le daemon et le webserver d'une instance dédiée.
