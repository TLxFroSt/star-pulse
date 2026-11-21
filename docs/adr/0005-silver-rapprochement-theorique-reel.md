# 0005 — Silver : rapprochement des passages théoriques et temps réel

- Statut : accepté
- Date : 2026-11-21

## Contexte

Pour mesurer la ponctualité, chaque passage observé dans le flux GTFS-RT doit être
rattaché à son horaire théorique (GTFS statique). Le flux STAR a des particularités,
constatées sur des données réelles :

- `trip_id` identique à celui de `trips.txt`, mais `start_date` toujours vide : le jour
  de service n'est pas fourni. Un même `trip_id` circule plusieurs jours.
- `stop_time_update` donne `stop_id` et une heure **absolue** (`time`, + `uncertainty`),
  sans `stop_sequence` ni `delay`. Les arrêts `SKIPPED` n'ont pas d'heure.
- Les horaires théoriques vont jusqu'à `31:58:00` : une course d'un jour de service peut
  passer le lendemain matin, en même temps que les premières courses du jour suivant.
- Le flux contient des courses « figées » : mise à jour vieille de plusieurs heures,
  prévisions décalées d'exactement un jour.
- Il contient aussi des prévisions « plates » : tous les arrêts d'une course à la même
  heure, publiées pour toutes les courses à venir d'un même service de véhicule
  (jusqu'à 8 h d'« avance » si on les prenait au pied de la lettre).

## Décision

- **dbt-duckdb**, sources lues directement sur MinIO (`read_parquet` + `httpfs`). Staging
  et intermédiaires dans un fichier DuckDB local ; la table silver centrale
  `int_stop_passages_matched` est matérialisée en Parquet dans `s3://lake/silver/`
  (matérialisation `external`).
- **Heure théorique** : `HH:MM:SS` → secondes, ajoutées à « midi moins 12 h » du jour de
  service en `Europe/Paris` (convention GTFS, correcte aux changements d'heure).
- **Passage observé** = dernière prévision connue pour (course, arrêt). Un passage n'est
  gardé que s'il est révolu (heure observée — ou théorique pour un arrêt supprimé —
  antérieure au dernier instantané reçu).
- **Courses figées écartées** : mise à jour de la course plus vieille que 30 min par
  rapport à l'instantané (`rt_max_update_age_minutes`).
- **Prévisions plates écartées** : une version de course dont au moins 3 arrêts ont
  tous la même heure prévue est ignorée.
- **Runs** : les instantanés d'un même `trip_id` séparés de plus de 4 h sont des
  circulations distinctes (`rt_run_gap_hours`).
- **Jour de service** : on teste le jour local de l'observation et la veille, on garde
  les services actifs (calendar + exceptions de calendar_dates) et on retient le
  candidat le plus proche de l'horaire théorique. Même règle pour un arrêt desservi deux
  fois par une course (boucle).
- **Version du GTFS** : celle ingérée en dernier avant le jour de service (la plus
  ancienne couvre aussi les jours antérieurs).
- **Tests dbt** : unicité (générique maison `unique_combination_of_columns`, sans
  package externe), relations vers arrêts / lignes / courses, valeurs acceptées,
  bornes de retard −30 / +90 min et part de passages non rapprochés < 5 % (en
  avertissement), fraîcheur du bronze temps réel (avertissement après 10 min).
- **Lancement via un wrapper Python** (`star_pulse.transform.dbt_runner`) : dbt ne lit
  pas `.env` ; le wrapper charge `Settings` et passe les valeurs à `profiles.yml` par
  variables d'environnement (secret S3 préfixé `DBT_ENV_SECRET_` pour être masqué).

## Conséquences

- Premier run sur données réelles : ≈ 99,6 % des passages révolus rapprochés, retard
  médian ≈ +30 s. Quelques courses très en avance (> 30 min) restent signalées par le
  test d'avertissement plutôt que filtrées.
- `gtfs-realtime-bindings` est limité à `< 3` : la version 3 exige `protobuf >= 7`,
  incompatible avec dbt-core 1.x (`protobuf < 7`). Les deux lisent le même format.
- La reconstruction est complète à chaque run (volumétrie faible). L'incrémental par
  jour de service sera ajouté avec l'orchestration (étape 6).
- Une boucle dont le même arrêt apparaît deux fois dans la même circulation ne garde
  que le dernier passage : limite acceptée, cas rare sur le réseau.
