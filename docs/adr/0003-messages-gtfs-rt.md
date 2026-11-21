# 0003 — Messages GTFS-RT dans Redpanda : format et déduplication

- Statut : accepté
- Date : 2026-11-21

## Contexte

Les trois flux GTFS-RT STAR (trip updates, vehicle positions, alerts) sont des instantanés
complets (`FULL_DATASET`) en protobuf, sans ETag. Interrogés toutes les 30 s, ils répètent
en grande partie le cycle précédent : environ la moitié des trip updates et la totalité des
alertes sont inchangées d'un cycle à l'autre (mesuré le 2026-11-21).

## Décision

- **Un message par entité**, pas un message par flux : les consumers et dbt travaillent
  au grain course / véhicule, et Kafka garantit l'ordre par clé.
- **Clé Kafka** = identifiant métier : `trip_id` (trip updates), `vehicle.id`
  (positions), `id` de l'entité (alertes).
- **Valeur en JSON** (protobuf converti par `MessageToDict`, noms de champs d'origine),
  dans une enveloppe : `feed`, `feed_timestamp`, `fetched_at`, `entity_key`,
  `entity_version`, `entity`. Lisible dans Redpanda Console et trivial à écrire en Parquet.
  Les entiers 64 bits (timestamps) y sont des chaînes, conformément au mapping JSON de
  protobuf : le typage est fait en silver.
- **Déduplication en deux niveaux**, côté producteur :
  1. timestamp d'en-tête inchangé → aucun message ;
  2. sinon, seules les entités dont la *version* a changé sont publiées. Version =
     timestamp de l'entité (trip updates, positions) ou hash du contenu (alertes).
  La mémoire n'est mise à jour qu'après acquittement du broker, et elle est remplacée à
  chaque cycle par l'instantané courant (bornée à la taille du flux).
- **Producteur idempotent** (`enable.idempotence`, `acks=all`) : pas de doublon lié aux
  retries réseau.
- **Topics** : 3 partitions, rétention 7 jours, créés au démarrage s'ils manquent.

## Conséquences

- Après un redémarrage du producteur, le premier cycle republie tout l'instantané. Les
  doublons sont éliminés en aval sur `(entity_key, entity_version)`, ce qui rend toute la
  chaîne idempotente sans état partagé.
- Passer à Avro / Protobuf + Schema Registry (déjà disponible dans Redpanda) reste possible
  si le volume ou le besoin de contrat de schéma le justifie.
