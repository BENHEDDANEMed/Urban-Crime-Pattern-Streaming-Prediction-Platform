"""
Liste des sources de crimes. Pour ajouter une ville :
  1. créer un fichier dans ce dossier (copier chicago.py par exemple)
  2. ajouter la ville dans common/config.py (CITIES)
  3. l'ajouter dans le dictionnaire ci-dessous
"""
from producer.sources.chicago import ChicagoSource
from producer.sources.los_angeles import LosAngelesSource
from producer.sources.new_york import NewYorkSource
from producer.sources.uk_police import UKPoliceSource

SOURCES = {
    "chicago": ChicagoSource,
    "los_angeles": LosAngelesSource,
    "new_york": NewYorkSource,
    "london": UKPoliceSource,
}