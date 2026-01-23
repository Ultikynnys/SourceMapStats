import os
import sys
from dotenv import load_dotenv

# ─── Environment Setup ────────────────────────────────────────────────────────
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Load environment variables
load_dotenv(os.path.join(BASE_DIR, '.env'))

# Ensure local libs are importable
sys.path.insert(0, os.path.join(BASE_DIR, "pythonvalve"))
sys.path.insert(0, os.path.join(BASE_DIR, "a2s"))
