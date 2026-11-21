# 0002 — Bronze GTFS théorique : structure et idempotence

- Statut : accepté
- Date : 2026-11-21

## Contexte

Le GTFS STAR est publié à une URL fixe et regénéré chaque nuit (~23h50), y compris quand les
horaires ne changent pas : le zip change alors d'octets (dates internes) sans changer de
contenu. Une version GTFS reste valable plusieurs semaines (`feed_start_date` →
`feed_end_date`) et le rapprochement théorique / réel doit pouvoir rejouer n'importe quel jour
avec la version alors en vigueur.

## Décision

- **Requête conditionnelle** (`If-None-Match` + ETag mémorisé) : pas de téléchargement inutile.
- **Checksum de contenu** : sha256 des fichiers triés (nom + sha256 de chaque fichier), et non
  du zip. Un ré-zippage nocturne est reconnu comme doublon.
- **Clé d'idempotence** : `bronze/gtfs_static/_versions/<sha256>.json`. Ce manifeste est écrit
  **en dernier** et sert de marqueur de commit : une exécution interrompue est rejouée sur les
  mêmes clés, sans doublon.
- **Structure** :
  ```
  bronze/gtfs_static/
  ├── _raw/ingestion_date=YYYY-MM-DD/version=<sha12>/gtfs.zip     # archive d'origine
  ├── <table>/ingestion_date=YYYY-MM-DD/version=<sha12>/<table>.parquet
  ├── _versions/<sha256>.json                                      # manifeste
  └── _state/latest.json                                           # dernier ETag vu
  ```
- **Parquet tout en texte** : bronze ne perd rien (zéros initiaux des identifiants, horaires
  au-delà de 24:00:00). Le typage est fait par les modèles dbt `stg_*`.
- **Conversion via DuckDB** (déjà dans la stack) : ~2 M de lignes `stop_times` en ~1 s.

## Conséquences

- Silver choisit la version applicable à une date de service via les manifestes
  (`feed.start_date` / `feed.end_date`).
- Le zip brut est conservé : on peut toujours reconstruire bronze si la conversion évolue.
- Coût de stockage : ~20 Mo par nouvelle version, seulement quand les horaires changent.
