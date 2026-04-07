"""FastAPI application entrypoint for the full API deployment."""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from api.app_factory import create_full_app

app = create_full_app()
