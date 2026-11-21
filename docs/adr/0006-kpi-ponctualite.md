# 0006 — Gold : définition des KPI de ponctualité

- Statut : accepté
- Date : 2026-11-21

## Contexte

Le silver (`int_stop_passages_matched`) donne, pour chaque passage révolu, son horaire
théorique et son retard en secondes. Le gold doit en tirer des indicateurs lisibles par
ligne, par heure, par jour et par arrêt, avec des définitions explicites et stables.

## Décision

- **Classes de ponctualité** (variables dbt `on_time_min_delay_s` / `on_time_max_delay_s`) :
  - à l'heure : retard dans **[−60 s, +180 s]**, bornes incluses ;
  - en avance : retard < −60 s (un bus en avance fait rater le bus : on le distingue) ;
  - en retard : retard > +180 s ;
  - supprimé : arrêt `SKIPPED` dans le temps réel.
- **Dénominateur** des pourcentages = passages observés. Les arrêts supprimés sont
  comptés à part (`n_skipped`) pour ne pas mélanger ponctualité et service non rendu.
- **Arrivées au terminus exclues** des tables KPI : les horaires y incluent une marge de
  battement, les bus arrivent souvent en avance et cela fausserait la mesure. Elles
  restent dans `fct_stop_passages` (colonne `is_terminus_arrival`).
- **Heure et jour** = ceux de l'horaire théorique, en heure de Paris, et le jour de
  service GTFS (une course de 0 h 30 appartient au jour de service précédent).
- **Indicateurs** : nombre de passages, % à l'heure / en avance / en retard, retard
  moyen, médian et 90e centile (`quantile_cont`), arrêts supprimés. Une macro
  `punctuality_metrics()` garantit les mêmes définitions dans toutes les tables.
- **Tables** (Parquet dans `s3://lake/gold/`) : `dim_routes`, `dim_stops` (version GTFS
  la plus récente), `fct_stop_passages` (grain passage), `fct_punctuality_route_day`,
  `fct_punctuality_route_hour`, `fct_punctuality_stop_day`.
- **Tests** : unicité des grains, relations vers les dimensions, pourcentages dans
  [0, 1], cohérence des totaux KPI avec la table de faits, et un unit test dbt sur la
  classification (cas limites −60 s / −61 s / +180 s / +181 s).

## Conséquences

- Les KPI sont additifs via les compteurs (`n_on_time`, `n_observed`…) : le dashboard
  peut réagréger plusieurs jours ou lignes sans moyenne de pourcentages.
- Le retard moyen et le p90 ne sont pas additifs : ils sont recalculés depuis
  `fct_stop_passages` quand on change de maille.
- Premier run (≈ 2 h de données, samedi midi) : ≈ 78 % des passages à l'heure sur le
  réseau, retard médian ≈ +35 s.
