"""
tests/test_read_service.py

This test suite verifies the basic functionality of the Read-Side Microservice
(src/read_service/app.py). The tests use Flask's built-in test client
to simulate HTTP requests, so no external client (like curl or Postman) is needed.

We don’t care about real database tables yet. These tests are focused on:
- The service being "alive" (health endpoint).
- The stub event processor returning a JSON response.
- The stub data query endpoint returning something structured, even if the DB is empty.
- Swagger documentation being available at /swagger.json.

To run:
    pytest -q tests/test_read_service.py
"""

import sys, os
import pytest   # <-- needed for fixtures
from flask import Flask

# Fix Python path so pytest can find src/
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from src.read_service.app import app  # Import the Flask app we wrote

# --- Test setup fixtures ---------------------------------------------------

@pytest.fixture
def client():
    """
    Pytest fixture: gives us a test client for the Flask app.
    The test client lets us make requests (GET, POST, etc.)
    without starting the real server on port 5001.
    """
    with app.test_client() as client:
        yield client


# --- Tests ----------------------------------------------------------------

def test_health(client):
    """
    Test the /health endpoint.
    - Makes a GET request to /health.
    - Verifies that the HTTP status code is 200 (success).
    - Checks that the JSON response contains "status".
    - This ensures the service is at least responding to requests.
    """
    resp = client.get("/health")
    assert resp.status_code == 200
    data = resp.get_json()
    assert "status" in data
    assert data["service"] == "read_service"


def test_event_stub(client):
    """
    Test the /events/<event_type> endpoint.
    - Sends a GET request with a fake event_type ("test").
    - Even though the 'events' table doesn’t exist yet,
      the app should still respond with JSON.
    - Valid responses can be HTTP 200 (OK) or 500 (error),
      but it must be JSON so clients don’t break.
    """
    resp = client.get("/events/test")
    assert resp.status_code in (200, 500)
    data = resp.get_json()
    # Either we got "events" (happy path) or "error" (DB missing).
    assert "events" in data or "error" in data


def test_data_stub(client):
    """
    Test the /data/<data_type> endpoint.
    - Requests /data/routes, which is supposed to pull route info from PostgreSQL.
    - Since we don’t have a 'routes' table yet, the service should still
      return valid JSON, not crash.
    - Expect status code 200 or 500, and response must contain either
      "data" (when working) or "error" (when DB not ready).
    """
    resp = client.get("/data/routes")
    assert resp.status_code in (200, 500)
    data = resp.get_json()
    assert "data" in data or "error" in data


def test_swagger_json(client):
    """
    Test the /swagger.json endpoint.
    - This endpoint exposes the OpenAPI spec for our read_service.
    - A well-formed JSON spec should contain an "openapi" field.
    - This ensures future developers can view API docs in Swagger UI.
    """
    resp = client.get("/swagger.json")
    assert resp.status_code == 200
    data = resp.get_json()
    assert "openapi" in data

def test_query_stub(client):
    """
    Test the /query-stub endpoint.

    Purpose:
    - This endpoint is a required placeholder from the assignment spec.
    - It does not connect to the database or Kafka.
    - It simply returns a fixed JSON message so the read_service
      proves it has a query endpoint in place.

    What we check:
    - The HTTP response status is 200 (success).
    - The response JSON matches exactly:
        {"message": "This is a query stub endpoint"}
    """
    resp = client.get("/query-stub")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data == {"message": "This is a query stub endpoint"}