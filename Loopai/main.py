import asyncio
import uuid
from enum import Enum
from typing import List, Dict
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, conlist
from datetime import datetime
import heapq
import time

app = FastAPI()

class Priority(str, Enum):
    HIGH = "HIGH"
    MEDIUM = "MEDIUM"
    LOW = "LOW"

class IngestionRequest(BaseModel):
    ids: conlist(int, min_items=1, max_items=1000)
    priority: Priority = Priority.MEDIUM

class BatchStatus(str, Enum):
    YET_TO_START = "yet_to_start"
    TRIGGERED = "triggered"
    COMPLETED = "completed"

# In-memory storage
ingestion_jobs: Dict[str, dict] = {}
task_queue = []
processing_lock = asyncio.Lock()
is_processing = False

# Simulate external API call
async def simulate_external_api(id: int):
    await asyncio.sleep(1)  # Simulate network delay
    return {"id": id, "data": "processed"}

# Process a batch
async def process_batch(ingestion_id: str, batch_id: str, ids: List[int]):
    async with processing_lock:
        ingestion_jobs[ingestion_id]["batches"][batch_id]["status"] = BatchStatus.TRIGGERED
        results = []
        for id in ids:
            result = await simulate_external_api(id)
            results.append(result)
        ingestion_jobs[ingestion_id]["batches"][batch_id]["status"] = BatchStatus.COMPLETED
        update_ingestion_status(ingestion_id)
    return results

# Update overall ingestion status
def update_ingestion_status(ingestion_id: str):
    batches = ingestion_jobs[ingestion_id]["batches"]
    statuses = [batch["status"] for batch in batches.values()]
    
    if all(status == BatchStatus.COMPLETED for status in statuses):
        ingestion_jobs[ingestion_id]["status"] = BatchStatus.COMPLETED
    elif any(status == BatchStatus.TRIGGERED for status in statuses):
        ingestion_jobs[ingestion_id]["status"] = BatchStatus.TRIGGERED
    else:
        ingestion_jobs[ingestion_id]["status"] = BatchStatus.YET_TO_START

# Background task to process queue
async def process_queue():
    global is_processing
    while True:
        async with processing_lock:
            if not task_queue or is_processing:
                await asyncio.sleep(0.1)
                continue
            
            is_processing = True
            _, ingestion_id, batch_id, ids = heapq.heappop(task_queue)
            await process_batch(ingestion_id, batch_id, ids)
            is_processing = False
            await asyncio.sleep(5)  # Rate limit: 1 batch per 5 seconds

# Start queue processing
@app.on_event("startup")
async def startup_event():
    asyncio.create_task(process_queue())

@app.post("/ingest")
async def ingest_data(request: IngestionRequest):
    ingestion_id = str(uuid.uuid4())
    
    # Validate IDs
    for id in request.ids:
        if not (1 <= id <= 10**9 + 7):
            raise HTTPException(status_code=400, detail=f"ID {id} out of valid range")
    
    # Create batches of 3 IDs
    batches = {}
    for i in range(0, len(request.ids), 3):
        batch_ids = request.ids[i:i+3]
        batch_id = str(uuid.uuid4())
        batches[batch_id] = {
            "ids": batch_ids,
            "status": BatchStatus.YET_TO_START
        }
    
    # Store ingestion job
    ingestion_jobs[ingestion_id] = {
        "status": BatchStatus.YET_TO_START,
        "batches": batches,
        "created_time": time.time(),
        "priority": request.priority
    }
    
    # Enqueue batches with priority
    priority_value = {"HIGH": 0, "MEDIUM": 1, "LOW": 2}[request.priority]
    for batch_id, batch in batches.items():
        # Negative priority for max-heap behavior
        heapq.heappush(task_queue, (-priority_value, ingestion_id, batch_id, batch["ids"]))
    
    return {"ingestion_id": ingestion_id}

@app.get("/status/{ingestion_id}")
async def get_status(ingestion_id: str):
    if ingestion_id not in ingestion_jobs:
        raise HTTPException(status_code=404, detail="Ingestion ID not found")
    
    job = ingestion_jobs[ingestion_id]
    return {
        "ingestion_id": ingestion_id,
        "status": job["status"],
        "batches": [
            {
                "batch_id": batch_id,
                "ids": batch["ids"],
                "status": batch["status"]
            }
            for batch_id, batch in job["batches"].items()
        ]
    }