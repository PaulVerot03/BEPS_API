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

allowed_origins_raw = os.getenv("ALLOWED_ORIGINS", "http://localhost:5173")
allowed_origins = [origin.strip() for origin in allowed_origins_raw.split(",") if origin.strip()]

app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Helper to convert MongoDB-like queries to ArangoDB AQL filters
def mongo_query_to_aql(query: dict):
    clauses = []
    bind_vars = {}
    var_idx = 0
    
    for key, value in query.items():
        # Map fields to doc paths: e.g. metrics.chain -> doc.metrics.chain
        path_parts = key.split(".")
        arango_path = "doc." + ".".join(path_parts)
        
        if isinstance(value, dict):
            for op, op_val in value.items():
                var_name = f"val_{var_idx}"
                var_idx += 1
                if op == "$gt":
                    clauses.append(f"{arango_path} > @{var_name}")
                elif op == "$lt":
                    clauses.append(f"{arango_path} < @{var_name}")
                elif op == "$gte":
                    clauses.append(f"{arango_path} >= @{var_name}")
                elif op == "$lte":
                    clauses.append(f"{arango_path} <= @{var_name}")
                elif op == "$eq":
                    clauses.append(f"{arango_path} == @{var_name}")
                bind_vars[var_name] = op_val
        else:
            var_name = f"val_{var_idx}"
            var_idx += 1
            clauses.append(f"{arango_path} == @{var_name}")
            bind_vars[var_name] = value
            
    filter_str = "FILTER " + " AND ".join(clauses) if clauses else ""
    return filter_str, bind_vars


# MongoDB Adapter Classes
class MongoCursorAdapter:
    def __init__(self, mongo_cursor):
        self.cursor = mongo_cursor

    def skip(self, skip_val: int):
        self.cursor = self.cursor.skip(skip_val)
        return self

    def limit(self, limit_val: int):
        self.cursor = self.cursor.limit(limit_val)
        return self

    def __aiter__(self):
        return self.cursor.__aiter__()


class MongoCollectionAdapter:
    def __init__(self, mongo_collection):
        self.collection = mongo_collection

    def parse_id(self, sequence_id: str):
        try:
            return ObjectId(sequence_id)
        except Exception:
            raise ValueError("Invalid sequence ID format")

    async def insert_one(self, document: dict):
        class InsertResult:
            def __init__(self, inserted_id):
                self.inserted_id = inserted_id
        result = await self.collection.insert_one(document)
        return InsertResult(result.inserted_id)

    def find(self, query: dict = None):
        if query is None:
            query = {}
        cursor = self.collection.find(query)
        return MongoCursorAdapter(cursor)

    async def find_one(self, query: dict, projection: dict = None):
        doc = await self.collection.find_one(query, projection)
        return doc

    async def update_one(self, query: dict, update_dict: dict):
        class UpdateResult:
            def __init__(self, matched_count):
                self.matched_count = matched_count
        result = await self.collection.update_one(query, update_dict)
        return UpdateResult(result.matched_count)


# ArangoDB Adapter Classes
class ArangoCursorAdapter:
    def __init__(self, db, query_filter, collection_name="sequences"):
        self.db = db
        self.query_filter = query_filter
        self.collection_name = collection_name
        self._skip = 0
        self._limit = None
        self._results = None
        self._index = 0

    def skip(self, skip_val: int):
        self._skip = skip_val
        return self

    def limit(self, limit_val: int):
        self._limit = limit_val
        return self

    async def _fetch(self):
        import anyio
        
        def run_query():
            filter_str, bind_vars = mongo_query_to_aql(self.query_filter)
            limit_str = ""
            if self._skip != 0 or self._limit is not None:
                skip = self._skip
                limit = self._limit if self._limit is not None else 999999999
                limit_str = f"LIMIT {skip}, {limit}"
                
            aql = f"""
            FOR doc IN {self.collection_name}
                {filter_str}
                {limit_str}
                RETURN doc
            """
            cursor = self.db.aql.execute(aql, bind_vars=bind_vars)
            results = []
            for doc in cursor:
                doc["_id"] = doc["_key"]
                results.append(doc)
            return results

        self._results = await anyio.to_thread.run_sync(run_query)

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self._results is None:
            await self._fetch()
        if self._index >= len(self._results):
            raise StopAsyncIteration
        item = self._results[self._index]
        self._index += 1
        return item


class ArangoCollectionAdapter:
    def __init__(self, db, collection_name="sequences"):
        self.db = db
        self.collection_name = collection_name
        self.collection = db.collection(collection_name)

    def parse_id(self, sequence_id: str):
        # ArangoDB keys are strings and don't need validation like ObjectId.
        return sequence_id

    async def insert_one(self, document: dict):
        import anyio
        
        def run_insert():
            res = self.collection.insert(document)
            return res["_key"]
            
        key = await anyio.to_thread.run_sync(run_insert)
        
        class InsertResult:
            def __init__(self, inserted_id):
                self.inserted_id = inserted_id
        return InsertResult(key)

    def find(self, query: dict = None):
        if query is None:
            query = {}
        return ArangoCursorAdapter(self.db, query, self.collection_name)

    async def find_one(self, query: dict, projection: dict = None):
        import anyio
        
        def run_find_one():
            if len(query) == 1 and ("_id" in query or "_key" in query):
                key = query.get("_id") or query.get("_key")
                if isinstance(key, str):
                    try:
                        doc = self.collection.get(key)
                        if doc:
                            doc["_id"] = doc["_key"]
                            if projection:
                                doc = {k: v for k, v in doc.items() if projection.get(k) == 1}
                            return doc
                    except Exception:
                        pass
            
            filter_str, bind_vars = mongo_query_to_aql(query)
            aql = f"""
            FOR doc IN {self.collection_name}
                {filter_str}
                LIMIT 1
                RETURN doc
            """
            cursor = self.db.aql.execute(aql, bind_vars=bind_vars)
            docs = list(cursor)
            if docs:
                doc = docs[0]
                doc["_id"] = doc["_key"]
                if projection:
                    doc = {k: v for k, v in doc.items() if projection.get(k) == 1}
                return doc
            return None

        return await anyio.to_thread.run_sync(run_find_one)

    async def update_one(self, query: dict, update_dict: dict):
        import anyio
        
        key = query.get("_id") or query.get("_key")
        if not key:
            raise NotImplementedError("ArangoDB update query must specify _id or _key")
            
        update_data = update_dict.get("$set", update_dict)
        update_data["_key"] = key
        
        def run_replace():
            try:
                self.collection.replace(update_data)
                return 1
            except Exception:
                return 0
                
        matched_count = await anyio.to_thread.run_sync(run_replace)
        
        class UpdateResult:
            def __init__(self, matched_count):
                self.matched_count = matched_count
        return UpdateResult(matched_count)


async def get_collection():
    db_type = os.getenv("DB_TYPE", "mongodb").lower()
    
    if db_type == "arangodb":
        from arango import ArangoClient
        import anyio
        
        ARANGO_URL = os.getenv("ARANGO_URL", "http://localhost:8529")
        ARANGO_USER = os.getenv("ARANGO_USER", "root")
        ARANGO_PASSWORD = os.getenv("ARANGO_PASSWORD", "")
        ARANGO_DB = os.getenv("ARANGO_DB", "ARN")
        
        def connect_arango():
            client = ArangoClient(hosts=ARANGO_URL)
            try:
                db = client.db(ARANGO_DB, username=ARANGO_USER, password=ARANGO_PASSWORD)
                if not db.has_collection("sequences"):
                    db.create_collection("sequences")
            except Exception as direct_error:
                try:
                    sys_db = client.db('_system', username=ARANGO_USER, password=ARANGO_PASSWORD)
                    if not sys_db.has_database(ARANGO_DB):
                        sys_db.create_database(ARANGO_DB)
                    db = client.db(ARANGO_DB, username=ARANGO_USER, password=ARANGO_PASSWORD)
                    if not db.has_collection("sequences"):
                        db.create_collection("sequences")
                except Exception as sys_error:
                    raise direct_error
            return db
            
        db = await anyio.to_thread.run_sync(connect_arango)
        return ArangoCollectionAdapter(db)
        
    else:
        MONGO_URI = os.getenv("API_USER")
        mongo_tls_env = os.getenv("MONGO_TLS")
        if mongo_tls_env is None:
            mongo_tls = True
        else:
            mongo_tls = mongo_tls_env.lower() in ("true", "1", "yes")
        client = AsyncMongoClient(MONGO_URI, tls=False)
        db = client["arn"]
        return MongoCollectionAdapter(db["sequences"])


def find_directory(env_var_name: str, candidates: List[str], base_dir: Optional[str] = None) -> str:
    env_path = os.getenv(env_var_name)
    if env_path:
        env_path = os.path.expanduser(env_path)
        if os.path.isdir(env_path):
            return env_path

    if base_dir is None:
        base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

    for candidate in candidates:
        candidate_path = os.path.abspath(os.path.join(base_dir, candidate))
        if os.path.isdir(candidate_path):
            return candidate_path

    raise FileNotFoundError(
        f"Could not find path for {env_var_name}. Checked env var {env_var_name}={env_path} and candidates: {candidates} in {base_dir}"
    )


#@app.post("/sequences/", response_model=SequenceResponseModel, status_code=status.HTTP_201_CREATED)
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
        parsed_id = collection.parse_id(sequence_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
        
    doc = await collection.find_one({"_id": parsed_id})
    if doc:
        doc["_id"] = str(doc["_id"])
        return doc
    raise HTTPException(status_code=404, detail="Sequence not found")


@app.put("/sequences/{sequence_id}", response_model=SequenceResponseModel)
async def update_sequence(sequence_id: str, data: SequenceModel, collection = Depends(get_collection)):
    try:
        parsed_id = collection.parse_id(sequence_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
        
    update_data = data.model_dump()
    result = await collection.update_one({"_id": parsed_id}, {"$set": update_data})
    
    if result.matched_count == 0:
        raise HTTPException(status_code=404, detail="Sequence not found")
        
    updated_doc = await collection.find_one({"_id": parsed_id})
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
        parsed_id = collection.parse_id(sequence_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    
    doc = await collection.find_one(
        {"_id": parsed_id},
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


@app.get("/sequences/{sequence_id}/video")
async def get_video(sequence_id: str, collection = Depends(get_collection)):
    try:
        parsed_id = collection.parse_id(sequence_id)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    
    doc = await collection.find_one(
        {"_id": parsed_id},
        {"file": 1, "_id": 0}
    )
    
    if not doc:
        raise HTTPException(status_code=404, detail="Sequence not found")
    
    file_path = doc.get("file", "")
    file_path = file_path.replace("outputs/", "")
    
    # Construct the video path by replacing the filename with folding_animation.mp4
    parts = file_path.split("/")
    if len(parts) > 1:
        parts[-1] = "folding_animation.mp4"
        video_path = "/".join(parts)
    else:
        video_path = "folding_animation.mp4"
        
    ovh_url = f"https://bucket.paulverot.fr/PDB/{video_path}"
    print(f"URL OVH Video : {ovh_url}")  
    
    return RedirectResponse(url=ovh_url)


@app.api_route("/calcul/{sequence}", methods=["GET", "POST"])
async def calcul_sequence(sequence: str, collection = Depends(get_collection)):
    sequence = sequence.upper()
    existe = await collection.find_one({"sequence": sequence})
    if existe:
        existe["_id"] = str(existe["_id"])
        return existe

    try:
        RNA_PATH = find_directory(
            "RNA_PATH",
            [
                "Optimize_3D_ARNStructure",
                "Optimize_3D_ARNStructure-main",
                #os.path.join("Optimize_3D_ARNStructure-main", "Optimize_3D_ARNStructure"),
                os.path.join("up2date", "OptimizeRNA"),
                "/home/blender/app/up2date/Optimize_3D_ARNStructure",
            ],
        )
    except FileNotFoundError as exc:
        raise HTTPException(status_code=500, detail=str(exc))

    try:
        BEPS_PATH = find_directory(
            "BEPS_PATH",
            ["BEPS", "BEPS_API"],
        )
    except FileNotFoundError as exc:
        raise HTTPException(status_code=500, detail=str(exc))

    rna_python = os.getenv("RNA_PYTHON", sys.executable)
    env = os.environ.copy()
    env["CUDA_LAUNCH_BLOCKING"] = "1"
    env["TF_CPP_MIN_LOG_LEVEL"] = "2"
    
    try:
        launch_script_path = os.path.join(RNA_PATH, "ignore", "launch.sh")
        result_rna = subprocess.run(
            [
                "bash",
                launch_script_path,
                sequence
            ],
            cwd=RNA_PATH,
            env=env,
            check=True,
            capture_output=True,
            text=True,
        )
    except FileNotFoundError as exc:
        raise HTTPException(
            status_code=500,
            detail=f"RNA executable not found: {exc}",
        )
    except subprocess.CalledProcessError as exc:
        stdout = exc.stdout.strip() if exc.stdout else "<no stdout>"
        stderr = exc.stderr.strip() if exc.stderr else "<no stderr>"
        raise HTTPException(
            status_code=500,
            detail=(
                f"RNA calculation failed in {RNA_PATH}\n"
                f"command: {exc.cmd}\n"
                f"returncode: {exc.returncode}\n"
                f"stdout: {stdout}\n"
                f"stderr: {stderr}"
            ),
        )

    db_type = os.getenv("DB_TYPE", "mongodb").lower()
    beps_script = "get_data_arango.py" if db_type == "arangodb" else "get_data.py"
    
    try:
        result_beps = subprocess.run(
            [
                sys.executable,
                beps_script,
                "--sequence",
                sequence,
            ],
            cwd=BEPS_PATH,
            check=True,
            capture_output=True,
            text=True,
        )
    except subprocess.CalledProcessError as exc:
        raise HTTPException(
            status_code=500,
            detail=(
                f"BEPS calculation failed in {BEPS_PATH}: {exc.stdout}\n{exc.stderr}"
            ),
        )

    result = await collection.find_one({"sequence": sequence})
    if result:
        result["_id"] = str(result["_id"])
        return result

    raise HTTPException(status_code=500, detail="Calcul échoué") 