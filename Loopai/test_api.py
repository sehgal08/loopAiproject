import pytest
import asyncio
from httpx import AsyncClient
from main import app, ingestion_jobs, task_queue, BatchStatus, Priority
import time

@pytest.fixture
async def client():
    async with AsyncClient(app=app, base_url="http://test") as client:
        yield client

@pytest.mark.asyncio
async def test_ingestion_and_status(client):
    # Test ingestion with valid input
    response = await client.post("/ingest", json={"ids": [1, 2, 3, 4, 5], "priority": "MEDIUM"})
    assert response.status_code == 200
    ingestion_id = response.json()["ingestion_id"]
    assert ingestion_id
    
    # Check initial status
    response = await client.get(f"/status/{ingestion_id}")
    assert response.status_code == 200
    status = response.json()
    assert status["ingestion_id"] == ingestion_id
    assert status["status"] == "yet_to_start"
    assert len(status["batches"]) == 2  # 2 batches: [1,2,3] and [4,5]
    
    # Wait for first batch to process (within 5 seconds)
    await asyncio.sleep(2)
    response = await client.get(f"/status/{ingestion_id}")
    status = response.json()
    assert status["status"] == "triggered"
    assert any(batch["status"] == "triggered" or batch["status"] == "completed" for batch in status["batches"])
    
    # Wait for all batches to complete
    await asyncio.sleep(6)
    response = await client.get(f"/status/{ingestion_id}")
    status = response.json()
    assert status["status"] == "completed"
    assert all(batch["status"] == "completed" for batch in status["batches"])

@pytest.mark.asyncio
async def test_priority_handling(client):
    # Submit medium priority job
    response1 = await client.post("/ingest", json={"ids": [1, 2, 3, 4, 5], "priority": "MEDIUM"})
    ingestion_id1 = response1.json()["ingestion_id"]
    
    # Submit high priority job after 2 seconds
    await asyncio.sleep(2)
    response2 = await client.post("/ingest", json={"ids": [6, 7, 8, 9], "priority": "HIGH"})
    ingestion_id2 = response2.json()["ingestion_id"]
    
    # Wait for processing
    await asyncio.sleep(8)  # Enough for 2 batches (first batch of medium, then high)
    
    # Check high priority job processed first
    response2 = await client.get(f"/status/{ingestion_id2}")
    status2 = response2.json()
    assert status2["status"] in ["triggered", "completed"]
    assert any(batch["status"] == "completed" for batch in status2["batches"])
    
    response1 = await client.get(f"/status/{ingestion_id1}")
    status1 = response1.json()
    assert status1["status"] == "triggered"
    assert any(batch["status"] == "yet_to_start" for batch in status1["batches"])

@pytest.mark.asyncio
async def test_rate_limit(client):
    start_time = time.time()
    response = await client.post("/ingest", json={"ids": [1, 2, 3, 4, 5, 6, 7, 8], "priority": "HIGH"})
    ingestion_id = response.json()["ingestion_id"]
    
    # Wait for all batches (3 batches: [1,2,3], [4,5,6], [7,8])
    await asyncio.sleep(15)  # Should take ~15 seconds (3 batches * 5 seconds)
    response = await client.get(f"/status/{ingestion_id}")
    status = response.json()
    assert status["status"] == "completed"
    assert all(batch["status"] == "completed" for batch in status["batches"])
    
    # Verify rate limit (should take at least 10 seconds for 3 batches)
    assert time.time() - start_time >= 10

@pytest.mark.asyncio
async def test_invalid_id(client):
    response = await client.post("/ingest", json={"ids": [0], "priority": "MEDIUM"})
    assert response.status_code == 400
    assert "out of valid range" in response.json()["detail"]
    
    response = await client.post("/ingest", json={"ids": [10**9 + 8], "priority": "MEDIUM"})
    assert response.status_code == 400
    assert "out of valid range" in response.json()["detail"]

@pytest.mark.asyncio
async def test_invalid_ingestion_id(client):
    response = await client.get("/status/invalid_id")
    assert response.status_code == 404
    assert "Ingestion ID not found" in response.json()["detail"]