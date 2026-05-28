# pyrefly: ignore [missing-import]
from dotenv import load_dotenv  # pyright: ignore[reportMissingImports]
import os
from fastapi import FastAPI, Depends, HTTPException, status  # pyright: ignore[reportMissingImports]
from pydantic import BaseModel, Field  # pyright: ignore[reportMissingImports]
from typing import List, Optional
from bson import ObjectId  # pyright: ignore[reportMissingImports]
from pymongo import AsyncMongoClient # pyright: ignore[reportMissingImports]
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.responses import RedirectResponse
import subprocess
import sys

load_dotenv()  

class MetricsModel(BaseModel):
    methods: Optional[str] = ""
    score_function: Optional[str] = ""
    length: Optional[int] = 0
    bead_atom: Optional[str] = ""
    chain: Optional[str] = ""
    time: Optional[float] = 0.0
    gpu_time: Optional[float] = 0.0
    final_score: Optional[float] = 0.0
    best_score_step: Optional[int] = 0
    molecule: Optional[str] = ""
    local_filepath: Optional[str] = ""
    potential: Optional[float] = 0.0
    bond: Optional[float] = 0.0

class InterframeModel(BaseModel):
    phase: Optional[str] = ""
    epoch: Optional[str] = ""
    score: Optional[float] = 0.0
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

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],
    allow_methods=["*"],
    allow_headers=["*"],
)

async def get_collection():
    
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


#retourner toutes les sequaneces 
@app.get("/sequences/", response_model=List[SequenceResponseModel])
async def list_sequences(limit: int = 10, skip: int = 0, collection = Depends(get_collection)):
    cursor = collection.find().skip(skip).limit(limit)
    results = []
    async for doc in cursor:
        doc["_id"] = str(doc["_id"])
        results.append(doc)
    return results


@app.get("/sequences/{sequence_id}", response_model=SequenceResponseModel)
async def by_sequence(sequence_id: str, collection = Depends(get_collection)):
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

@app.get("/chain")
async def by_chain(chain: str = "R", collection = Depends(get_collection)):
    query = {"metrics.chain": chain}
    cursor = collection.find(query)
    results = []
    async for doc in cursor:
        doc["_id"] = str(doc["_id"])
        results.append(doc)
    return results

@app.get("/vers")
async def by_vers(vers: str = "1.0", collection = Depends(get_collection)):
    query = {"vers": vers}
    cursor = collection.find(query)
    results = []
    async for doc in cursor:
        doc["_id"] = str(doc["_id"])
        results.append(doc)
    return results

@app.get("/score/{lower},{upper}")
async def by_score_range(lower: float, upper:float , collection = Depends(get_collection)):
    query = {"metrics.final_score":{"$gt": float(lower), "$lt":float(upper)}}
    cursor = collection.find(query)
    results = []
    async for doc in cursor:
        doc["_id"] = str(doc["_id"])
        results.append(doc)
    return results
    
@app.get("/organism/{organisme_name}")
async def by_organism(organisme_name: str, collection = Depends(get_collection)):
    query = {"organism": organisme_name}
    cursor = collection.find(query)
    results = []
    async for doc in cursor:
        doc["_id"] = str(doc["_id"])
        results.append(doc)
    return results

@app.get("/name/{sequence_name}")
async def by_name(sequence_name: str, collection = Depends(get_collection)):
    query = {"name": sequence_name}
    cursor = collection.find(query)
    results = []
    async for doc in cursor:
        doc["_id"] = str(doc["_id"])
        results.append(doc)
    return results



#chercher par sequence arn specific 
@app.get("/sequence/{arn_sequence}")
async def by_arn_sequence(arn_sequence: str, collection = Depends(get_collection)):
    query = {"sequence": arn_sequence.upper()}
    cursor = collection.find(query)
    results = []
    async for doc in cursor:
        doc["_id"] = str(doc["_id"])
        results.append(doc)
    return results


#chercher par methode 
@app.get("/method/{method_name}")
async def by_method(method_name: str, collection = Depends(get_collection)):
    query = {"metrics.methods": method_name}
    cursor = collection.find(query)
    results = []
    async for doc in cursor:
        doc["_id"] = str(doc["_id"])
        results.append(doc)
    return results


#recherche par atom
@app.get("/bead_atom/{bead_atom}")
async def by_bead_atom(bead_atom: str, collection = Depends(get_collection)):
    query = {"metrics.bead_atom": bead_atom}
    cursor = collection.find(query)
    results = []
    async for doc in cursor:
        doc["_id"] = str(doc["_id"])
        results.append(doc)
    return results


#recherche par longueur de sequence 
@app.get("/length/{length}")
async def by_length(length: int, collection = Depends(get_collection)):
    query = {"metrics.length": length}
    cursor = collection.find(query)
    results = []
    async for doc in cursor:
        doc["_id"] = str(doc["_id"])
        results.append(doc)
    return results


#recuperer des pdb individuellement

@app.get("/sequences/{sequence_id}/pdb")
async def get_pdb(sequence_id: str, collection = Depends(get_collection)):
    try:
        obj_id = ObjectId(sequence_id)
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid sequence ID format")
    
    doc = await collection.find_one(
        {"_id": obj_id},
        {"file": 1, "_id": 0}
    )
    
    if not doc:
        raise HTTPException(status_code=404, detail="Sequence not found")
    
    '''OUTPUTS_PATH = os.getenv("OUTPUTS_PATH", ".")
    pdb_path = os.path.join(OUTPUTS_PATH, doc.get("file", ""))
    print(f"Chemin cherché : {pdb_path}")
    
    if not os.path.exists(pdb_path):
        raise HTTPException(status_code=404, detail="PDB file not found on server")
    
    return FileResponse(
        path=pdb_path,
        filename=os.path.basename(pdb_path),
        media_type="chemical/x-pdb"
    )'''
    file_path = doc.get("file", "")
    file_path = file_path.replace("outputs/", "")
    ovh_url = f"https://bucket.paulverot.fr/PDB/{file_path}"
    print(f"URL OVH : {ovh_url}")  
    
    return RedirectResponse(url=ovh_url)


@app.post("/calcul/{sequence}")
async def calcul_sequence(sequence: str, collection = Depends(get_collection)):
    existe = await collection.find_one({"sequence": sequence})
    if existe:
        existe["_id"] = str(existe["_id"])
        return existe

    RNA_PATH =  os.getenv("RNA_PATH", os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "Optimize_3D_ARNStructure")))
    BEPS_PATH = os.getenv("BEPS_PATH", os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "BEPS")))
    
    result_rna = subprocess.run([
        sys.executable, "main.py", "launch",
        "--input-val", sequence,
        "--batch",
        "--score", "RASP",
        "--molecule", "RNA",
        "--visualise",
        "--save-metrics"
    ], cwd=RNA_PATH)
    
    result_beps = subprocess.run([
        sys.executable, "get_data.py",
        "--sequence", sequence 
        ], cwd=BEPS_PATH)

    result = await collection.find_one({"sequence": sequence})
    if result:
        result["_id"] = str(result["_id"])
        return result
    
    raise HTTPException(status_code=500, detail="Calcul échoué") 