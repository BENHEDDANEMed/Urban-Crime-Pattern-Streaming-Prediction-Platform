"""
DBSCAN : trouver les zones où beaucoup de crimes sont proches les uns des autres.

Comment marche DBSCAN, en simple :
  - On choisit une distance "eps" (ex: 0.5 km) et un nombre "min_samples" (ex: 5).
  - Si un crime a au moins 5 crimes autour de lui à moins de 0.5 km,
    c'est le centre d'un groupe (un "cluster").
  - Les crimes isolés sont du "bruit" (label -1) : on les ignore.
  - Avantage : pas besoin de dire à l'avance combien de groupes on veut
    (contrairement à K-Means).

Distance : on travaille avec latitude/longitude, donc on utilise la distance
"haversine" (distance réelle sur la Terre, qui est ronde).

Ce fichier est en Python pur (pas de Spark) : on peut le tester facilement
avec pytest sur son PC.
"""
from __future__ import annotations

import math
from collections import Counter
from datetime import datetime, timezone

import numpy as np
from sklearn.cluster import DBSCAN

EARTH_RADIUS_KM = 6371.0088
SCHEMA_VERSION = 1


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Distance en km entre deux points GPS."""
    lat1, lon1, lat2, lon2 = map(math.radians, (lat1, lon1, lat2, lon2))
    a = (math.sin((lat2 - lat1) / 2) ** 2
         + math.cos(lat1) * math.cos(lat2) * math.sin((lon2 - lon1) / 2) ** 2)
    return 2 * EARTH_RADIUS_KM * math.asin(math.sqrt(a))


def find_hotspots(points: list[dict], eps_km: float = 0.5, min_samples: int = 5,
                  max_radius_km: float | None = None) -> list[dict]:
    """
    points = liste de crimes : [{"latitude":..., "longitude":..., "crime_category":...}, ...]
    Renvoie une liste de hotspots (du plus gros au plus petit).

    max_radius_km : on ignore les groupes plus grands que ce rayon.
    Pourquoi ? "Effet de chaîne" de DBSCAN : dans une ville très dense, les crimes
    se touchent de proche en proche (A proche de B, B proche de C...) et DBSCAN
    peut fusionner tout un quartier en un seul groupe de 10 km. Ce n'est pas un hotspot.
    """
    points = [p for p in points if p.get("latitude") is not None and p.get("longitude") is not None]
    if len(points) < min_samples:
        return []

    # haversine dans scikit-learn attend des RADIANS, et eps aussi en radians.
    coords = np.radians([[p["latitude"], p["longitude"]] for p in points])
    labels = DBSCAN(
        eps=eps_km / EARTH_RADIUS_KM,
        min_samples=min_samples,
        metric="haversine",
        algorithm="ball_tree",
    ).fit_predict(coords)

    hotspots = []
    for label in sorted(set(labels) - {-1}):  # -1 = bruit
        members = [p for p, lab in zip(points, labels) if lab == label]
        center_lat = sum(p["latitude"] for p in members) / len(members)
        center_lon = sum(p["longitude"] for p in members) / len(members)
        radius = max(haversine_km(center_lat, center_lon, p["latitude"], p["longitude"])
                     for p in members)
        if max_radius_km is not None and radius > max_radius_km:
            continue  # trop étendu : ce n'est pas une zone précise
        categories = Counter(p.get("crime_category") or "other" for p in members)
        hotspots.append({
            "crime_count": len(members),
            "center_latitude": round(center_lat, 6),
            "center_longitude": round(center_lon, 6),
            "radius_km": round(radius, 3),
            "top_category": categories.most_common(1)[0][0],
            "categories": dict(categories),
        })
    hotspots.sort(key=lambda h: h["crime_count"], reverse=True)
    return hotspots


def severity(crime_count: int, min_samples: int) -> str:
    """'high' si le groupe est au moins 3 fois plus gros que le minimum, sinon 'medium'."""
    return "high" if crime_count >= 3 * min_samples else "medium"


def build_alerts(city: str, window_start, window_end, points: list[dict],
                 eps_km: float = 0.5, min_samples: int = 5,
                 max_radius_km: float | None = None) -> list[dict]:
    """Transforme les hotspots d'une fenêtre de temps en messages d'alerte (topic crimes.alerts)."""
    detected_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    start_text = window_start.strftime("%Y-%m-%dT%H:%M:%SZ") if hasattr(window_start, "strftime") \
        else str(window_start)
    end_text = window_end.strftime("%Y-%m-%dT%H:%M:%SZ") if hasattr(window_end, "strftime") \
        else str(window_end)

    alerts = []
    for rank, hotspot in enumerate(find_hotspots(points, eps_km, min_samples, max_radius_km)):
        # Id lisible : ville + début de fenêtre + position arrondie (~1 km)
        zone = f"{hotspot['center_latitude']:.2f}_{hotspot['center_longitude']:.2f}"
        alerts.append({
            "schema_version": SCHEMA_VERSION,
            "alert_id": f"{city}|{start_text}|{zone}",
            "city": city,
            "window_start": start_text,
            "window_end": end_text,
            "rank": rank + 1,
            "severity": severity(hotspot["crime_count"], min_samples),
            "eps_km": eps_km,
            "min_samples": min_samples,
            "max_radius_km": max_radius_km,
            "detected_at": detected_at,
            **hotspot,
        })
    return alerts