# Architecture Decision Records

Chaque décision structurante du projet est consignée ici : le contexte, ce qui a été
décidé, et ce que cela implique. Un ADR accepté n'est pas réécrit ; une décision qui
change donne lieu à un nouvel ADR qui remplace l'ancien.

| N° | Décision | Étape |
|---|---|---|
| [0001](0001-stack-locale.md) | Stack locale : Redpanda, MinIO, DuckDB/dbt, Dagster | 0 |
| [0002](0002-bronze-gtfs-theorique.md) | Bronze GTFS théorique : structure et idempotence | 1 |
| [0003](0003-messages-gtfs-rt.md) | Messages GTFS-RT dans Redpanda : format et déduplication | 2 |
| [0004](0004-bronze-gtfs-rt.md) | Bronze temps réel : at-least-once, partitionnement et nommage | 3 |
| [0005](0005-silver-rapprochement-theorique-reel.md) | Silver : rapprochement des passages théoriques et temps réel | 4 |
| [0006](0006-kpi-ponctualite.md) | Gold : définition des KPI de ponctualité | 5 |
| [0007](0007-orchestration-dagster.md) | Orchestration Dagster | 6 |
| [0008](0008-ci-et-jeu-de-demo.md) | CI GitHub Actions et jeu de démonstration | 8 |
| [0009](0009-dashboard-streamlit.md) | Dashboard Streamlit | 7 |

Format : titre, statut, date, puis **Contexte**, **Décision**, **Conséquences**.
