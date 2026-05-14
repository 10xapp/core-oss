"""FastAPI application entrypoint for the dedicated webhook ingress service."""

import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from api.app_factory import create_webhooks_app


app = create_webhooks_app()
