# pyrefly: ignore [missing-import]
from dotenv import load_dotenv
import os
# pyrefly: ignore [missing-import]
from fastapi import FastAPI, Depends, HTTPException, status
from pydantic import BaseModel, Field
from typing import List, Optional
# pyrefly: ignore [missing-import]
from bson import ObjectId
# pyrefly: ignore [missing-import]
from pymongo import AsyncMongoClient

class MetricsModel(BaseModel):
    methods: Optional[str] = ""
    score_function: Optional[str] = ""
    length: Optional[str] = ""
    bead_atom: Optional[str] = ""
    chain: Optional[str] = ""
    time: Optional[str] = ""
    gpu_time: Optional[str] = ""
    final_score: Optional[str] = ""
    best_score_step: Optional[str] = ""
    molecule: Optional[str] = ""
    local_filepath: Optional[str] = ""
    potential: Optional[str] = ""
    bond: Optional[str] = ""

class InterframeModel(BaseModel):
    phase: Optional[str] = ""
    epoch: Optional[str] = ""
    score: Optional[str] = ""
    video_path: Optional[str] = ""
    pdb_path: Optional[str] = ""

class SequenceModel(BaseModel):
    sequence: Optional[str] = ""
    name: Optional[str] = ""
    organism: Optional[str] = ""
    date: Optional[str] = ""
    vers: Optional[str] = ""
    file: Optional[str] = ""
    metrics: Optional[MetricsModel] = None
    RMSD: Optional[str] = ""
    RMSD_avg: Optional[float] = None
    RMSD_std: Optional[float] = None
    interframes: Optional[List[InterframeModel]] = []

class SequenceResponseModel(SequenceModel):
    id: str = Field(alias="_id")

    model_config = {
        "populate_by_name": True
    }

app = FastAPI(title="Sequence API")
async def get_collection():
    load_dotenv()
    MONGO_URI = os.getenv("API_USER")
    client = AsyncMongoClient(MONGO_URI, tls=True)
    db = client["anais"]
    return db["sequence"]

@app.post("/sequences/", response_model=SequenceResponseModel, status_code=status.HTTP_201_CREATED)
async def create_sequence(data: SequenceModel, collection = Depends(get_collection)):
    sequence_dict = data.model_dump()
    result = await collection.insert_one(sequence_dict)
    sequence_dict["_id"] = str(result.inserted_id)
    return sequence_dict

@app.get("/sequences/", response_model=List[SequenceResponseModel])
async def list_sequences(limit: int = 10, skip: int = 0, collection = Depends(get_collection)):
    cursor = collection.find().skip(skip).limit(limit)
    results = []
    async for doc in cursor:
        doc["_id"] = str(doc["_id"])
        results.append(doc)
    return results

@app.get("/sequences/{sequence_id}", response_model=SequenceResponseModel)
async def get_sequence(sequence_id: str, collection = Depends(get_collection)):
    try:
        obj_id = ObjectId(sequence_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid sequence ID format")
        
    doc = await collection.find_one({"_id": obj_id})
    if doc:
        doc["_id"] = str(doc["_id"])
        return doc
    raise HTTPException(status_code=404, detail="Sequence not found")

@app.put("/sequences/{sequence_id}", response_model=SequenceResponseModel)
async def update_sequence(sequence_id: str, data: SequenceModel, collection = Depends(get_collection)):
    try:
        obj_id = ObjectId(sequence_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid sequence ID format")
        
    update_data = data.model_dump()
    result = await collection.update_one({"_id": obj_id}, {"$set": update_data})
    
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="Sequence not found")
        
    updated_doc = await collection.find_one({"_id": obj_id})
    updated_doc["_id"] = str(updated_doc["_id"])
    return updated_doc

# @app.delete("/sequences/{sequence_id}", status_code=status.HTTP_204_NO_CONTENT)
# async def delete_sequence(sequence_id: str, collection = Depends(get_collection)):
#     try:
#         obj_id = ObjectId(sequence_id)
#     except Exception:
#         raise HTTPException(status_code=400, detail="Invalid sequence ID format")
        
#     result = await collection.delete_one({"_id": obj_id})
#     if result.deleted_count == 0:
#         raise HTTPException(status_code=404, detail="Sequence not found")

@app.get("/test")
async def test_query(chain: str = "R", collection = Depends(get_collection)):
    """Original test route ported to new collection structure"""
    query = {"metrics.chain": chain}
    cursor = collection.find(query)
    results = []
    async for doc in cursor:
        doc["_id"] = str(doc["_id"])
        results.append(doc)
    return results