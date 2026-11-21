# STAR Pulse

**Les bus de Rennes sont-ils à l'heure ?** STAR Pulse est un pipeline de data engineering
qui ingère en continu les flux temps réel GTFS-RT du réseau STAR, les confronte aux horaires
théoriques GTFS et calcule des KPI de ponctualité par ligne, arrêt et heure, exposés dans un
dashboard.

Tout tourne en local avec Docker Compose, à coût zéro, sur des données open data sans
authentification.

> Statut : **Étape 3 — consumer Redpanda vers bronze Parquet.** Voir la [feuille de route](#feuille-de-route).

## Architecture

```mermaid
flowchart LR
    RT["GTFS-RT STAR<br/>(protobuf, temps réel)"] --> P["Producer<br/>star_pulse.ingestion"]
    P --> K[("Redpanda<br/>topics GTFS-RT")]
    K --> C["Consumer<br/>star_pulse.streaming"]
    C --> B[("MinIO s3://lake<br/>bronze · Parquet")]
    GTFS["GTFS théorique<br/>(zip quotidien)"] --> B
    B --> S["dbt-duckdb<br/>silver : rapprochement<br/>théorique / réel"]
    S --> G["dbt-duckdb<br/>gold : KPI ponctualité"]
    G --> D["Streamlit<br/>KPI + carte"]
    O{{"Dagster<br/>assets · schedules · checks"}} -.-> P & C & S & G
```

| Couche | Outil | Rôle |
|---|---|---|
| Bus de messages | Redpanda (API Kafka) + Console | découple la collecte temps réel du stockage |
| Stockage | MinIO (S3) + Parquet `date=/hour=` | lakehouse bronze / silver / gold |
| Transformations | DuckDB + dbt-duckdb | modèles `stg_*`, `int_*`, `fct_*` / `dim_*` testés |
| Orchestration | Dagster | assets, schedules, sensors, asset checks |
| Restitution | Streamlit | KPI et carte des retards |

Les choix de stack sont expliqués dans [`docs/adr/`](docs/adr/).

## Démarrage rapide

Prérequis : [uv](https://docs.astral.sh/uv/), Docker (avec Compose v2) et `make`.

```bash
cp .env.example .env        # puis adapter si besoin
make install                # dépendances Python 3.12 + hooks pre-commit
make up                     # Redpanda, Redpanda Console, MinIO + bucket "lake"
make lint && make test
```

| Service | URL |
|---|---|
| Redpanda (Kafka) | `localhost:19092` |
| Redpanda Console | http://localhost:8080 |
| MinIO API S3 | http://localhost:9000 |
| MinIO Console | http://localhost:9001 |

`make down` arrête l'infra (les volumes sont conservés).

Première ingestion :

```bash
make ingest-gtfs   # télécharge le GTFS STAR et l'écrit en bronze (Parquet + zip brut)
make ingest-gtfs   # relance : « GTFS inchangé (ETag) », rien n'est réécrit
```

Temps réel :

```bash
make produce-rt    # terminal 1 — Ctrl+C pour arrêter ; messages sur http://localhost:8080
make consume-rt    # terminal 2 — écrit un lot Parquet par minute dans MinIO
```

Silver (après quelques minutes de temps réel) :

```bash
make dbt-build     # staging + rapprochement théorique / réel, tests dbt
make dbt-docs      # documentation et lignage sur http://localhost:8081
```

La table centrale `int_stop_passages_matched` (un passage révolu par ligne, avec son
horaire théorique et son retard en secondes) est écrite en Parquet dans
`s3://lake/silver/`. Les règles de rapprochement sont décrites dans
[l'ADR 0005](docs/adr/0005-silver-rapprochement-theorique-reel.md).

## Commandes

| Commande | Effet |
|---|---|
| `make up` / `make down` | démarre / arrête l'infra Docker |
| `make lint` | `ruff check` + `ruff format --check` + `mypy --strict` |
| `make fmt` | formatage automatique |
| `make test` | tests unitaires (`pytest`) |
| `make test-integration` | tests d'intégration contre MinIO (après `make up`) |
| `make ingest-gtfs` | ingère le GTFS théorique STAR dans `s3://lake/bronze/gtfs_static/` |
| `make produce-rt` | interroge les flux GTFS-RT toutes les 30 s et publie sur Redpanda |
| `make consume-rt` | consomme Redpanda et écrit le Parquet bronze (`s3://lake/bronze/gtfs_rt/`) |
| `make dbt-build` | construit et teste le silver dbt (`s3://lake/silver/`) |
| `make dbt-freshness` | alerte si le bronze temps réel a plus de 10 min |
| `make dbt-docs` | documentation dbt sur http://localhost:8081 |

## Structure

```
src/star_pulse/   code Python (config, ingestion, streaming, transform, common)
dbt/              projet dbt (bronze / silver / gold)
orchestration/    définitions Dagster
dashboard/        app Streamlit
infra/terraform/  bonus : déploiement Azure
docs/adr/         Architecture Decision Records
tests/            unit/, integration/, fixtures/
```

## Sources de données

Open data du réseau STAR (Rennes Métropole), via transport.data.gouv.fr et data.gouv.fr :
horaires théoriques GTFS, flux GTFS-RT *trip updates*, *vehicle positions* et *alerts*.
La collecte respecte les sources : polling ≥ 30 s, User-Agent explicite, retries avec backoff.

## Feuille de route

- [x] **Étape 0** — Squelette : arborescence, uv, Docker Compose, Makefile, pre-commit, smoke test
- [x] **Étape 1** — Ingestion GTFS théorique → bronze (checksum de contenu, idempotence)
- [x] **Étape 2** — Producteur GTFS-RT → Redpanda (déduplication, producteur idempotent)
- [x] **Étape 3** — Consumer Redpanda → Parquet sur MinIO (at-least-once, partitions date/heure)
- [x] **Étape 4** — dbt silver : rapprochement théorique / réel
- [ ] **Étape 5** — dbt gold : KPI de ponctualité
- [ ] **Étape 6** — Orchestration Dagster
- [ ] **Étape 7** — Dashboard Streamlit
- [ ] **Étape 8** — CI GitHub Actions, README complet, ADR
