"""
Tests for the write service Python app.
"""

import sys
import pathlib

# Get the Flask app
sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))
from src.write_service.app import app

# Load environment variables
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass
    
def test_health():
    """Test basic health of write service Flask app (test to see if connection and endpoints work)."""

    # Create a test client so we can simulate a connection
    app.testing = True
    client = app.test_client()

    # Let's test the health endpoint
    response = client.get("/health")

    # If the response is as expected, then we are good to go
    assert response.status_code == 200
    assert response.get_json() == {"status": "ok"}