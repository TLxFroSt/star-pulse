# 0004 — Bronze temps réel : at-least-once, partitionnement et nommage

- Statut : accepté
- Date : 2026-11-21

## Contexte

Les messages GTFS-RT arrivent en continu sur Redpanda (≈ 300 à 500 par cycle de 30 s). Ils
doivent être historisés en Parquet sur MinIO, sans perte, en fichiers de taille raisonnable,
et pouvoir être relus par DuckDB / dbt en ne lisant que les heures utiles.

## Décision

- **Lots** : écriture dès 5 000 messages ou 60 s (configurable). Assez gros pour éviter
  une myriade de petits fichiers, assez court pour une fraîcheur d'une minute.
- **Commit manuel après écriture** (`enable.auto.commit = false`) : les offsets ne sont
  commités qu'une fois *tous* les fichiers du lot écrits. Garantie **at-least-once** : un
  crash ou une erreur S3 entraîne une relecture, jamais une perte.
- **Partitionnement sur le temps de l'événement** (`feed_timestamp`, UTC) et non sur l'heure
  d'écriture : un retard de consommation ne déplace pas les données dans une autre heure.
  ```
  bronze/gtfs_rt/<feed>/date=YYYY-MM-DD/hour=HH/part-p<partition>-o<début>-<fin>.parquet
  ```
- **Nom de fichier déterministe** (partition et plage d'offsets) : relire exactement les
  mêmes offsets réécrit le même objet au lieu de créer un doublon.
- **Schéma bronze** : enveloppe typée (`feed`, `feed_timestamp`, `fetched_at`,
  `entity_key`, `entity_version`) + entité brute en JSON (`entity_json`) + métadonnées Kafka
  (`kafka_topic`, `kafka_partition`, `kafka_offset`, `kafka_timestamp_ms`) et
  `ingested_at`. L'aplatissement est fait en silver (`json_extract` de DuckDB) : un
  changement de format du flux ne casse pas l'ingestion.
- **Messages illisibles** : conservés dans `bronze/gtfs_rt/_rejected/`, jamais ignorés.

## Conséquences

- Des doublons restent possibles (lot relu partiellement après crash, producteur
  redémarré). Silver dédoublonne sur `(entity_key, entity_version)` : c'est ce couple, et non
  le fichier, qui fait office de clé d'idempotence de bout en bout.
- Un seul consumer suffit au volume actuel ; le groupe Kafka permet d'en ajouter jusqu'à
  3 (une par partition) sans changer le code.
