# 0009 — Dashboard Streamlit

- Statut : accepté
- Date : 2026-11-21

## Contexte

Le gold (ADR 0006) contient la table de faits `fct_stop_passages` (un passage par
ligne) et des tables KPI pré-agrégées par ligne et jour, ligne et heure, arrêt et
jour. Le dashboard doit répondre à « les bus sont-ils à l'heure ? » pour n'importe
quelle combinaison de période, type de jour, lignes et arrêts, et rester lisible par
quelqu'un qui ne connaît pas le GTFS.

## Décision

- **Calcul au grain passage** : chaque indicateur est recalculé par DuckDB (en
  mémoire, lecture du Parquet gold sur S3) depuis `fct_stop_passages`, avec les
  filtres en paramètres SQL. Pour toute sélection, les pourcentages sont des ratios
  de comptes et le retard médian et le p90 sont des centiles exacts, jamais des
  moyennes de pourcentages ou de centiles. Les tables KPI pré-agrégées restent pour
  les usages SQL et le lignage.
- **Le dashboard ne lit que le gold**, jamais le silver ni le bronze. Une connexion
  par session, des résultats en cache 5 min (le gold est reconstruit toutes les
  15 min par Dagster).
- **Arrêts regroupés par nom et commune** : STAR publie un `stop_id` par quai ou par
  sens. Le filtre et la carte montrent un arrêt « logique » (position moyenne des
  quais) qui porte la liste de ses `stop_id`.
- **Carte cliquable** (pydeck, sélection d'un objet) : cliquer un arrêt filtre toute
  la page et ouvre sa fiche (ponctualité, lignes qui le desservent, derniers
  passages). Un bouton efface la sélection. À noter : pydeck évalue les chaînes comme
  des expressions JavaScript, une valeur littérale s'écrit donc `"'pixels'"`.
- **Lecture des couleurs** :
  - une seule couleur d'accent pour les barres, une rampe séquentielle bleue pour la
    carte de chaleur jour × heure ;
  - couleurs de statut réservées aux classes de ponctualité, toujours accompagnées
    d'un libellé (en avance, à l'heure, en retard) ;
  - arrêts de la carte en quatre paliers de part à l'heure : 85 % et plus, 70 à 85 %,
    55 à 70 %, moins de 55 % ;
  - thème clair fixé dans `.streamlit/config.toml`.
- **Contexte de chaque chiffre** : écart avec la période précédente de même durée,
  tendance par jour sous chaque indicateur, ligne de moyenne sur les graphiques,
  badge de fraîcheur (dernière observation de moins de 30 min).
- **Volumes minimaux** : une heure avec moins de 20 passages, une ligne avec moins
  de 20 et un arrêt sous le seuil choisi (10 par défaut) ne sont pas affichés, pour
  ne pas montrer un 0 % ou un 100 % calculé sur deux passages.
- **Tests** : `streamlit.testing.AppTest` sur un gold de test (rendu complet,
  sélection d'un arrêt, état vide) et tests unitaires des requêtes et des formats.

## Conséquences

- N'importe quel filtre donne des chiffres exacts, au prix d'une requête sur la
  table de faits. C'est rapide tant que l'historique tient en quelques semaines ;
  au-delà, il faudrait partitionner `fct_stop_passages` par jour ou passer par les
  tables KPI pour les longues périodes.
- L'historique couvre seulement les jours où le producteur et le consumer ont
  tourné : STAR n'archive pas son temps réel.
- Le fond de carte (Carto) demande un accès Internet.
