# test.py

import pytest
from fastapi.testclient import TestClient
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from server import app

# Use the existing PostgreSQL database for tests
TEST_DATABASE_URL = "postgresql://nandinichatterjee:postgresql_tutorial@localhost/points"

# Create an engine and session for the test database
engine = create_engine(TEST_DATABASE_URL)
TestingSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


# Override the get_db dependency for testing
@pytest.fixture
def override_get_db():
    # Use a transaction for each test, and roll back changes afterward
    db = TestingSessionLocal()
    try:
        yield db
    finally:
        db.rollback()
        db.close()


# Test the create_user endpoint
def test_create_user(override_get_db):
    # Additional setup code if needed
    client = TestClient(app)
    response = client.post("/create_user/testuser1", headers={"Content-Type": "application/json"})
    # Print or log the response content for debugging
    print(response.content)
    assert response.status_code == 200


# Test the earn_points endpoint

def test_earn_points(override_get_db):
    # Additional setup code if needed
    client = TestClient(app)
    response = client.post("/earn_points/testuser/Type/10", headers={"Content-Type": "application/json"})
    assert response.status_code == 200


# Test the leaderboard endpoint
def test_leaderboard(override_get_db):
    # Additional setup code if needed
    client = TestClient(app)
    response = client.get("/leaderboard")
    assert response.status_code == 200
    expected_length = 5  # Update this to match the actual number of users in your test database
    assert len(response.json()) == expected_length
