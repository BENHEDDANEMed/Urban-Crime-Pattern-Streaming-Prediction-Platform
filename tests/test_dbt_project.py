"""
Phase 3 : cohérence entre le code Python et le projet dbt (sans lancer dbt).

Si quelqu'un ajoute une catégorie dans common/schemas.py sans l'ajouter dans le seed dbt,
le test relationships de fact_crime échouerait seulement avec les vraies données :
on le détecte ici tout de suite.
"""
import csv
from pathlib import Path

from common.config import CITIES
from common.schemas import CATEGORIES

DBT = Path(__file__).resolve().parent.parent / "crimewatch_dbt"


def read_seed(name: str) -> list[dict]:
    with open(DBT / "seeds" / f"{name}.csv", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def test_seed_categories_match_python():
    assert sorted(r["crime_category"] for r in read_seed("crime_categories")) == sorted(CATEGORIES)


def test_seed_cities_match_config():
    assert sorted(r["city"] for r in read_seed("cities")) == sorted(CITIES)


def test_seed_hours_cover_the_day():
    hours = read_seed("hours")
    assert [int(r["hour_of_day"]) for r in hours] == list(range(24))
