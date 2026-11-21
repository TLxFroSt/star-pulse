# 0008 — CI GitHub Actions et jeu de démonstration

- Statut : accepté
- Date : 2026-11-21

## Contexte

La CI doit prouver à chaque push que le code est propre et que le pipeline tourne de
bout en bout, tests dbt compris. Or le pipeline dépend de flux temps réel que STAR
n'archive pas : impossible de rejouer « la journée d'hier » en CI, et des données
réelles prises à un instant donné ne couvrent ni une semaine complète ni les cas
limites. Il faut aussi qu'une personne qui découvre le projet puisse voir le
dashboard sans attendre plusieurs jours de collecte.

## Décision

- **GitHub Actions, deux jobs en parallèle** (`.github/workflows/ci.yml`) :
  - `quality` : `uv sync --locked` (le lockfile doit être à jour), `ruff check`,
    `ruff format --check`, `mypy --strict`, tests unitaires ;
  - `pipeline` : Redpanda et MinIO via le même `docker-compose.yml` qu'en local,
    tests d'intégration, puis `make demo` (chargement du jeu de démo en bronze et
    `dbt build` : modèles silver et gold et tous les tests dbt).
- **Un test d'intégration sauté échoue en CI** : en local, ces tests se sautent si
  `make up` n'a pas été lancé ; en CI (variable `CI`), un saut voudrait dire une CI
  verte sans rien tester (`tests/integration/conftest.py`).
- **Réseau fictif versionné** dans `tests/fixtures/gtfs_demo/` : un vrai GTFS de
  2 lignes, 13 arrêts (dont une correspondance), 188 courses, services semaine et
  week-end, jours fériés dans `calendar_dates.txt`. Fictif plutôt qu'un extrait STAR :
  on maîtrise les horaires (journée et semaine complètes) et aucun chiffre de démo
  ne peut être pris pour une mesure réelle.
- **Temps réel simulé par du code** (`star_pulse.demo`), pas de Parquet commité :
  - chaque course a un retard qui dérive d'arrêt en arrêt, plus fort aux heures de
    pointe en semaine et sur la ligne B, avec des écarts d'un jour à l'autre, des
    incidents, des bus en avance et des arrêts supprimés ;
  - les instantanés imitent le flux STAR : seuls les arrêts à venir, un instantané
    toutes les 30 s au plus, le même `trip_id` chaque jour (plusieurs « runs »),
    `start_date` absent ;
  - les données passent par les fonctions de production : `ingest_gtfs_static`
    (téléchargement remplacé par un `httpx.MockTransport`), `build_batch`,
    `to_record`, `records_to_parquet`. Un changement du format bronze se répercute
    donc sur la démo et est testé par la CI ;
  - graine et semaine fixes (2 au 8 novembre 2026) : mêmes chiffres à chaque
    exécution.
- **Isolation** : bucket `demo` et base `data/demo.duckdb` (`make demo`,
  `make demo-dashboard`). Le chargement écrit un marqueur `_demo.json` et refuse
  un bucket qui contient déjà un GTFS sans ce marqueur : la démo ne peut pas se
  mélanger aux données réelles du bucket `lake`.
- **Actions figées** : `actions/checkout@v7`, `actions/upload-artifact@v7`,
  `astral-sh/setup-uv@v10.4.0` (cette action ne publie pas d'étiquette de version
  majeure). Cache uv activé, permissions en lecture seule, une seule exécution par
  branche à la fois. Si le job `pipeline` échoue, les logs de l'infra et de dbt sont
  publiés en artefact.

## Conséquences

- `make up && make demo && make demo-dashboard` donne un dashboard rempli en quelques
  minutes, sans connexion aux flux STAR. La CI exécute exactement ce chemin.
- Sur la démo, `dbt build` passe ses 91 nœuds (modèles et tests) sans avertissement ;
  la ponctualité simulée est d'environ 84 % (pointes vers 65 %, ligne B moins
  ponctuelle). Ces chiffres sont synthétiques.
- Deux défauts du flux STAR ne sont pas simulés : les courses figées et les prévisions
  « plates ». Leurs filtres (ADR 0005) ne sont exercés que sur des données réelles.
- La simulation suit notre compréhension du flux : elle teste la cohérence du
  pipeline, pas la justesse des hypothèses sur les données STAR.
