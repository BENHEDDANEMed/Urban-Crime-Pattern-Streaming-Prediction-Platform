"""Permet à pytest de trouver les dossiers common/, producer/, streaming/."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))