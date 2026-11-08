"""
CrimeWatch — dashboard Streamlit (phase 3)

  streamlit run dashboard/app.py          -> http://localhost:8501

4 onglets :
  🗺️ Carte          heatmap des crimes par zone (filtre heure / créneau) + zones les plus touchées
  🚨 Alertes live    hotspots DBSCAN du topic Kafka crimes.alerts (dernière version par alert_id)
  🔮 Prédictions     top-3 des catégories prédites par XGBoost pour une zone, un jour, une heure
  📊 Analyses        comparaison des villes, évolution, profil horaire, effet de la météo

Données : la base DuckDB construite par dbt (data/warehouse/crimewatch.duckdb, schéma marts)
et Kafka pour les alertes.
"""
import sys
from pathlib import Path

import altair as alt
import folium
import pandas as pd
import streamlit as st
from folium.plugins import HeatMap
from streamlit_folium import st_folium

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from dashboard.data import DUCKDB_PATH, KAFKA, WarehouseMissing, load_alerts, query  # noqa: E402

st.set_page_config(page_title="CrimeWatch", page_icon="🛰️", layout="wide")

CITY_CENTER = {
    "chicago": (41.8781, -87.6298),
    "los_angeles": (34.0522, -118.2437),
    "new_york": (40.7580, -73.9855),
    "london": (51.5130, -0.1340),
}
SLOT_HOURS = {"Toute la journée": range(24), "Nuit (0h-6h)": range(0, 6), "Matin (6h-12h)": range(6, 12),
              "Après-midi (12h-18h)": range(12, 18), "Soir (18h-24h)": range(18, 24)}
SEVERITY_COLOR = {"high": "#c0392b", "medium": "#e67e22"}
ACCENT = "#2f6fdf"


def base_map(city: str, zoom: int = 12) -> folium.Map:
    return folium.Map(location=CITY_CENTER[city], zoom_start=zoom, tiles="CartoDB positron", control_scale=True)


# ---------------------------------------------------------------------------- en-tête
st.title("🛰️ CrimeWatch")
st.caption("Criminalité urbaine en quasi temps réel — Chicago · Los Angeles · New York · Londres")

try:
    cities = query("select city, city_name, hour_reliable, first_crime_date, last_crime_date, crime_count "
                   "from marts.dim_city order by city_name")
except WarehouseMissing:
    st.error(f"Base introuvable : `{DUCKDB_PATH}`.\n\n"
             "Lance d'abord le pipeline (DAG Airflow `crimewatch_daily`) ou, à la main : "
             "`cd crimewatch_dbt` puis `dbt build`.")
    st.stop()

cities = cities[cities["crime_count"] > 0]
city_names = dict(zip(cities["city"], cities["city_name"]))

with st.sidebar:
    st.header("Filtres")
    city = st.selectbox("Ville", list(city_names), format_func=city_names.get)
    info = cities.set_index("city").loc[city]
    st.metric("Crimes dans l'entrepôt", f"{int(info['crime_count']):,}".replace(",", " "))
    st.caption(f"Du {pd.Timestamp(info['first_crime_date']):%d/%m/%Y} au {pd.Timestamp(info['last_crime_date']):%d/%m/%Y}")
    if not info["hour_reliable"]:
        st.info("Londres : la source ne donne que le mois du crime, pas l'heure. "
                "Les filtres par heure ne s'appliquent pas.")
    st.divider()
    if st.button("🔄 Recharger les données"):
        st.cache_data.clear()
        st.rerun()

tab_map, tab_alerts, tab_pred, tab_stats = st.tabs(["🗺️ Carte", "🚨 Alertes live", "🔮 Prédictions", "📊 Analyses"])

# ---------------------------------------------------------------------------- 🗺️ carte
with tab_map:
    c1, c2 = st.columns([1, 2])
    slot = c1.selectbox("Créneau", list(SLOT_HOURS), disabled=not info["hour_reliable"])
    top_n = c2.slider("Zones les plus touchées à afficher", 0, 50, 15)

    if info["hour_reliable"] and slot != "Toute la journée":
        hours = list(SLOT_HOURS[slot])
        heat = query(
            f"""select z.zone_lat, z.zone_lon, sum(h.crime_count) as crimes
                from marts.fact_zone_hourly h join marts.dim_zone z using (zone_id)
                where h.city = ? and h.hour_of_day in ({','.join(map(str, hours))})
                group by 1, 2""", (city,))
    else:
        heat = query("select zone_lat, zone_lon, crime_count as crimes from marts.dim_zone where city = ?", (city,))

    zones = query("""select z.zone_id, z.zone_lat, z.zone_lon, z.crime_count, z.rank_in_city,
                            z.share_of_city_pct, c.category_label_fr as top_category
                     from marts.dim_zone z join marts.dim_category c on c.crime_category = z.top_category
                     where z.city = ? and z.rank_in_city <= ? order by z.rank_in_city""", (city, max(top_n, 1)))

    m = base_map(city)
    if len(heat):
        HeatMap(heat[["zone_lat", "zone_lon", "crimes"]].astype(float).values.tolist(),
                radius=14, blur=18, min_opacity=0.25).add_to(m)
    for z in zones.head(top_n).itertuples():
        folium.CircleMarker(
            [z.zone_lat, z.zone_lon], radius=6, color="#1d1d1f", weight=1.5, fill=True, fill_color="white",
            fill_opacity=0.9,
            tooltip=f"#{z.rank_in_city} · {z.crime_count} crimes ({z.share_of_city_pct:.1f} % de la ville) · "
                    f"surtout : {z.top_category}",
        ).add_to(m)
    st_folium(m, height=560, use_container_width=True, returned_objects=[], key=f"map-{city}-{slot}")
    st.caption("Zones = cases de ~500 m. Cercles blancs = zones les plus touchées (survoler pour le détail).")

# ---------------------------------------------------------------------------- 🚨 alertes
with tab_alerts:
    st.caption(f"Hotspots détectés par DBSCAN dans des fenêtres de 30 min (topic Kafka `crimes.alerts`, {KAFKA}).")
    alerts = load_alerts()
    if alerts.empty:
        st.warning("Aucune alerte reçue. Kafka est-il lancé (`docker compose up -d kafka spark-streaming`) ? "
                   "Pour une démo : `PRODUCER_MODE=replay` puis `docker compose up -d producer`.")
    else:
        city_alerts = alerts[alerts["city"] == city]
        a1, a2, a3 = st.columns(3)
        a1.metric("Alertes (toutes villes)", len(alerts))
        a2.metric(f"Alertes · {city_names[city]}", len(city_alerts))
        a3.metric("Sévérité haute", int((city_alerts["severity"] == "high").sum()) if len(city_alerts) else 0)
        if len(city_alerts):
            m = base_map(city, zoom=11)
            for a in city_alerts.itertuples():
                folium.Circle(
                    [a.center_latitude, a.center_longitude], radius=max(a.radius_km, 0.15) * 1000,
                    color=SEVERITY_COLOR.get(a.severity, "#e67e22"), fill=True, fill_opacity=0.35,
                    tooltip=f"{a.severity.upper()} · {a.crime_count} crimes · surtout {a.top_category} · "
                            f"fenêtre {a.window_start:%d/%m %H:%M} UTC",
                ).add_to(m)
            st_folium(m, height=480, use_container_width=True, returned_objects=[], key=f"alerts-{city}")
            st.dataframe(
                city_alerts[["window_start", "severity", "crime_count", "top_category", "radius_km",
                             "center_latitude", "center_longitude"]],
                hide_index=True, use_container_width=True,
                column_config={"window_start": st.column_config.DatetimeColumn("Fenêtre (UTC)", format="DD/MM HH:mm"),
                               "severity": "Sévérité", "crime_count": "Crimes", "top_category": "Catégorie",
                               "radius_km": st.column_config.NumberColumn("Rayon (km)", format="%.2f")})
        else:
            st.info(f"Pas d'alerte pour {city_names[city]} pour l'instant.")

# ---------------------------------------------------------------------------- 🔮 prédictions
with tab_pred:
    if not info["hour_reliable"]:
        st.info("Pas de prédiction pour Londres : le modèle a besoin de l'heure des crimes.")
    else:
        preds_zones = query("""select distinct zone_id, rank_in_city from marts.mart_zone_predictions
                               where city = ? order by rank_in_city""", (city,))
        if preds_zones.empty:
            st.warning("Pas de prédictions : lancer `python -m ml.predict_zones` puis `dbt build`.")
        else:
            p1, p2, p3 = st.columns([2, 1, 2])
            zone = p1.selectbox("Zone (classée par nombre de crimes)", preds_zones["zone_id"],
                                format_func=lambda z: f"#{int(preds_zones.set_index('zone_id').loc[z, 'rank_in_city'])} · {z}")
            day_type = p2.radio("Jour", ["semaine", "week-end"], horizontal=True)
            hour = p3.slider("Heure", 0, 23, 21, format="%dh")

            top3 = query("""select prediction_rank, category_label_fr, probability, model_run_id
                            from marts.mart_zone_predictions
                            where zone_id = ? and day_type = ? and hour_of_day = ?
                            order by prediction_rank""", (zone, day_type, hour))
            left, right = st.columns([1, 1])
            with left:
                st.subheader(f"Top-3 à {hour}h ({day_type})")
                for r in top3.itertuples():
                    st.markdown(f"**{r.prediction_rank}. {r.category_label_fr}** — {r.probability:.0%}")
                    st.progress(float(r.probability))
                st.caption("Le modèle (XGBoost) trouve la bonne catégorie dans son top-3 environ 6 fois sur 10 "
                           "(phase 2 : top-3 = 0,62 contre 0,58 pour « les 3 catégories habituelles de la zone »).")
            with right:
                st.subheader("Catégorie n°1 prédite par zone")
                pmap = query("""select zone_lat, zone_lon, category_label_fr, probability
                                from marts.mart_zone_predictions
                                where city = ? and day_type = ? and hour_of_day = ? and prediction_rank = 1""",
                             (city, day_type, hour))
                labels = sorted(pmap["category_label_fr"].unique())
                palette = ["#2f6fdf", "#d64545", "#2a9d8f", "#e9a23b", "#7b4fd6", "#5c6b7a", "#c2549d", "#3b8f3b"]
                color = {lab: palette[i % len(palette)] for i, lab in enumerate(labels)}
                m = base_map(city, zoom=11)
                for r in pmap.itertuples():
                    folium.CircleMarker([r.zone_lat, r.zone_lon], radius=5, color=color[r.category_label_fr],
                                        fill=True, fill_opacity=0.85,
                                        tooltip=f"{r.category_label_fr} ({r.probability:.0%})").add_to(m)
                st_folium(m, height=380, use_container_width=True, returned_objects=[],
                          key=f"pred-{city}-{day_type}-{hour}")
                st.caption(" · ".join(f"<span style='color:{color[l]}'>●</span> {l}" for l in labels),
                           unsafe_allow_html=True)

# ---------------------------------------------------------------------------- 📊 analyses
with tab_stats:
    st.subheader("Comparaison des villes")
    comp = query("""select city_name as "Ville", crime_count as "Crimes", zone_count as "Zones",
                           crimes_per_day as "Crimes / jour", pct_hour_known as "% heure connue",
                           pct_night as "% de nuit", pct_weekend as "% week-end", pct_violent as "% violents",
                           c.category_label_fr as "Catégorie n°1"
                    from marts.mart_city_comparison m
                    join marts.dim_category c on c.crime_category = m.top_category
                    order by "Crimes" desc""")
    st.dataframe(comp, hide_index=True, use_container_width=True)

    s1, s2 = st.columns(2)
    with s1:
        st.subheader(f"Évolution · {city_names[city]}")
        daily = query("""select occurred_date, crime_count, violent_count from marts.mart_city_daily
                         where city = ? order by occurred_date""", (city,))
        daily = daily.melt("occurred_date", var_name="série", value_name="crimes")
        daily["série"] = daily["série"].map({"crime_count": "Tous les crimes", "violent_count": "Violents"})
        st.altair_chart(
            alt.Chart(daily).mark_line(strokeWidth=2).encode(
                x=alt.X("occurred_date:T", title=None),
                y=alt.Y("crimes:Q", title="Crimes par jour"),
                color=alt.Color("série:N", title=None, scale=alt.Scale(range=[ACCENT, "#d64545"])),
                tooltip=["occurred_date:T", "série:N", "crimes:Q"],
            ).properties(height=300), use_container_width=True)
    with s2:
        st.subheader("Profil horaire")
        if not info["hour_reliable"]:
            st.info("Pas d'heure fiable pour Londres.")
        else:
            prof = query("""select hour_of_day, day_type, avg_crimes_per_day from marts.mart_hourly_profile
                            where city = ? order by hour_of_day""", (city,))
            st.altair_chart(
                alt.Chart(prof).mark_line(point=True, strokeWidth=2).encode(
                    x=alt.X("hour_of_day:Q", title="Heure", scale=alt.Scale(domain=[0, 23])),
                    y=alt.Y("avg_crimes_per_day:Q", title="Crimes moyens par jour"),
                    color=alt.Color("day_type:N", title=None, scale=alt.Scale(range=[ACCENT, "#e9a23b"])),
                    tooltip=["hour_of_day:Q", "day_type:N", "avg_crimes_per_day:Q"],
                ).properties(height=300), use_container_width=True)

    st.subheader("Météo et criminalité")
    if not info["hour_reliable"]:
        st.info("Pas d'heure fiable pour Londres : impossible de relier un crime à la météo de son heure.")
    else:
        weather = query("""select rain_condition, temperature_band,
                                  sum(hours_observed) as hours, sum(crime_count) as crimes,
                                  sum(expected_crime_count) as expected
                           from marts.mart_weather_impact where city = ?
                           group by grouping sets ((rain_condition), (temperature_band))""", (city,))
        weather["condition"] = weather["rain_condition"].fillna(weather["temperature_band"])
        weather["index"] = weather["crimes"].astype(float) / weather["expected"].astype(float)
        weather = weather[weather["hours"] >= 24].sort_values("condition")     # trop peu d'heures : pas fiable
        st.altair_chart(
            alt.Chart(weather).mark_bar(color=ACCENT).encode(
                x=alt.X("index:Q", title="Crimes observés / attendus à la même heure de la journée"),
                y=alt.Y("condition:N", title=None, sort=None),
                tooltip=["condition:N", "hours:Q", "crimes:Q", alt.Tooltip("index:Q", format=".2f")],
            ).properties(height=260)
            + alt.Chart(pd.DataFrame({"x": [1]})).mark_rule(color="#1d1d1f", strokeDash=[4, 3]).encode(x="x:Q"),
            use_container_width=True)
        st.caption("1 = autant de crimes que d'habitude à ces heures-là. > 1 : plus de crimes pendant cette "
                   "condition météo. Les conditions observées moins de 24 h sont masquées.")
