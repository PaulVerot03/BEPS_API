from arango import ArangoClient  # pyright: ignore[reportMissingImports]
import pandas as pd  # pyright: ignore[reportMissingImports]
import json
import os
from dotenv import load_dotenv  # pyright: ignore[reportMissingImports]
from pprint import pprint


'''
This .py file will be called by the main task, it will be given a path to the specific vis_  dir.
In this dir are found :
    folding_vis.csv
    metrics.csv
From those we can get all needed infos. 
Those infos will be inserted into an ArangoDB database.
'''


def read_folding_vis(path):
    csv_path = path + '/folding_vis.csv'
    df = pd.read_csv(csv_path, skipinitialspace=True)
    return df

def read_metric(path):
    csv_path = path + '/metrics.csv'
    df = pd.read_csv(csv_path, skipinitialspace=True)
    df.columns = df.columns.str.strip()
    df = df.map(lambda x: x.strip() if isinstance(x, str) else x)
    return df

def parse_vis(vis_df):
    frames = []
    for _, row in vis_df.iterrows():
        interframe = {
            "phase": str(row.iloc[0]),
            "epoch": str(row.iloc[1]),
            "score": float(row.iloc[2]),
            "pdb_path": str(row.iloc[3])
        }
        frames.append(interframe)
    return frames 

def get_std_mean(vis_df):
    mean = vis_df["score"].mean()
    std = vis_df["score"].std()
    return std, mean
    

def parse_metrics(metrics_df):
    row = metrics_df.iloc[0]

    #verfication que la sequence contient que des lettres ARN valides 
    sequence = str(row.get('Sequence', ''))
    if not sequence or not all(c in 'augc' for c in sequence.lower()):
        print(f"Warning: Sequence '{sequence}' contains illegal letters or is empty. Skipping.")
        return None, None
    
    document = {
        "methods": str(row.get('Method', '')),
        "score_function": str(row.get('Score_Function', '')),
        "length": int(row.get('Sequence_Length', '')),
        "bead_atom": str(row.get('Bead_Atom', '')),
        "chain": str(row.get('Chain', '')),
        "time": float(row.get('Wall_Time_s', '')),
        "gpu_time": float(row.get('GPU_Time_s', '')),
        "video_path": "folding_animation.mp4",
        "final_score": float(row.get('Final_Score', '')),
        "best_score_step": int(row.get('Best_Score_Step', '')),
        "molecule": str(row.get('Molecule', '')),
        "local_filepath": str(row.get('Out_Name', '')),
        "potential": float(row.get('Potential', '')),
        "bond": float(row.get('Bond', ''))
    }
    
    
    top_level_info = {
        "sequence": str(row.get('Sequence', '')),
        "name": str(row.get('Name_Seq', '')),
        "organism": str(row.get('Organism', '')),
    }
    
    return document, top_level_info

def get_date(path):
    filename = os.path.basename(path)
    parts= filename.split("_")
    for part in parts:
        if len(part)==8 and part.isdigit():
            return f"{part[:4]}-{part[4:6]}-{part[6:]}"
    return ""


def check_and_get_version(sequence, bead_atom, chain, new_final_score, db, all_documents):
    # AQL query to retrieve documents matching sequence, bead_atom, and chain
    query = """
    FOR doc IN sequences
        FILTER doc.sequence == @sequence 
          AND doc.metrics.bead_atom == @bead_atom 
          AND doc.metrics.chain == @chain
        RETURN doc
    """
    cursor = db.aql.execute(query, bind_vars={
        "sequence": sequence,
        "bead_atom": bead_atom,
        "chain": chain
    })
    db_documents = list(cursor)
    
    local_documents = [doc for doc in all_documents if doc.get("sequence") == sequence and doc.get("metrics", {}).get("bead_atom") == bead_atom and doc.get("metrics", {}).get("chain") == chain]
    
    all_matching_docs = db_documents + local_documents
    
    if len(all_matching_docs) == 0:
        return "1.0"
        
    best_existing_score = float('inf')
    max_version = 0.0
    
    for doc in all_matching_docs:
        vers_str = doc.get("vers")
        if not vers_str:
            vers_str = "1.0"
            
        try:
            vers = float(vers_str)
        except ValueError:
            vers = 1.0
            
        if vers > max_version:
            max_version = vers
            
        metrics = doc.get("metrics", {})
        score_str = metrics.get("final_score")
        if score_str:
            try:
                score = float(score_str)
                if score < best_existing_score:
                    best_existing_score = score
            except ValueError:
                pass
                            
    if new_final_score < best_existing_score:
        print("yipeeeee")
        return str(round(max_version + 0.1, 1))
    else:
        return None
    
    
def prepare_send_to_arango(metrics, top_level_info, frames, avg, std, db, all_documents):
    sequence = top_level_info.get("sequence", "")
    bead_atom = metrics.get("bead_atom", "")
    chain = metrics.get("chain", "")
    try:
        new_final_score = float(metrics.get("final_score", float('inf')))
    except ValueError:
        new_final_score = float('inf')
        
    version = check_and_get_version(sequence, bead_atom, chain, new_final_score, db, all_documents)
    
    if version is None:
        return None
        
    last_pdb = frames[-1]["pdb_path"] if frames else ""
    document = {
        "sequence": sequence,
        "name": top_level_info.get("name", ""),
        "organism": top_level_info.get("organism", ""),
        "date": get_date(metrics.get("local_filepath", "")),
        "vers": version,
        "file": last_pdb,
        "metrics": metrics,
        "RMSD_avg": avg,
        "RMSD_std": std,
        "interframes": frames
    }
    return document


def main():
    load_dotenv()
    ARANGO_URL = os.getenv("ARANGO_URL", "http://localhost:8529")
    
    # Validate ARANGO_URL to avoid cryptic requests.exceptions.InvalidURL errors
    url_stripped = ARANGO_URL.strip().rstrip('/')
    if url_stripped in ("https:/", "https:", "https://", "http:/", "http:", "http://") or not url_stripped:
        print(f"Error: Invalid ARANGO_URL '{ARANGO_URL}' configured in .env. Please specify a valid host (e.g., 'http://localhost:8529' or your remote ArangoDB URL).")
        return

    ARANGO_USER = os.getenv("ARANGO_USER", "root")
    ARANGO_PASSWORD = os.getenv("ARANGO_PASSWORD", "")
    ARANGO_DB = os.getenv("ARANGO_DB", "ARN")

    client = ArangoClient(hosts=ARANGO_URL)
    
    # Try connecting directly to the target database and collection
    try:
        db = client.db(ARANGO_DB, username=ARANGO_USER, password=ARANGO_PASSWORD)
        if not db.has_collection("sequences"):
            collection = db.create_collection("sequences")
        else:
            collection = db.collection("sequences")
    except Exception as direct_error:
        # If that fails (e.g. database does not exist), try to connect to system and create it
        try:
            sys_db = client.db('_system', username=ARANGO_USER, password=ARANGO_PASSWORD)
            if not sys_db.has_database(ARANGO_DB):
                sys_db.create_database(ARANGO_DB)
            db = client.db(ARANGO_DB, username=ARANGO_USER, password=ARANGO_PASSWORD)
            if not db.has_collection("sequences"):
                collection = db.create_collection("sequences")
            else:
                collection = db.collection("sequences")
        except Exception as sys_error:
            print(f"Connection Error: Could not connect to database '{ARANGO_DB}' directly ({direct_error}) nor via '_system' database ({sys_error}).")
            raise direct_error
       
    origin_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "Optimize_3D_ARNStructure"))
       
    csv_file = "metrics.csv"
    source_file = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "Optimize_3D_ARNStructure", csv_file))

    vis_dirs = []
    if os.path.exists(source_file):
        import csv
        with open(source_file, 'r', encoding='utf-8') as f:
            reader = csv.reader(f)
            header = next(reader, None)
            if header:
                for row in reader:
                    if not row:
                        continue
                    last_val = row[-1].strip()
                    if last_val and ("vis_" in last_val or "outputs/" in last_val) and not last_val.endswith(".pdb") and not last_val.endswith(".cif"):
                        vis_dirs.append({"Vis_Dir": last_val})
    source = pd.DataFrame(vis_dirs)
    
    all_documents = []
    
    for i, row in source.iterrows():
        path = os.path.join(origin_path, row["Vis_Dir"])
        try:
            vis_df = read_folding_vis(path)
            metrics_df = read_metric(path)

            frames = parse_vis(vis_df)
            std, mean = get_std_mean(vis_df)
            metrics_dict, top_level_info = parse_metrics(metrics_df)
            
            if metrics_dict is None:
                continue
            
            final_document = prepare_send_to_arango(metrics_dict, top_level_info, frames, mean, std, db, all_documents)
            if final_document is not None:
                all_documents.append(final_document)
        except Exception as e:
            print(f"Error processing {path}: {e}")
            continue

    output_file = "arango_insert.json"
    with open(output_file, 'w') as f:
        json.dump(all_documents, f, indent=4)
        
    if all_documents:
        # Batch insert to avoid HTTP 413 Payload Too Large
        batch_size = 10
        inserted_count = 0
        for i in range(0, len(all_documents), batch_size):
            batch = all_documents[i:i + batch_size]
            try:
                collection.insert_many(batch)
                inserted_count += len(batch)
            except Exception as e:
                print(f"Batch insert of {len(batch)} documents failed (error: {e}). Retrying one-by-one...")
                for doc in batch:
                    try:
                        collection.insert(doc)
                        inserted_count += 1
                    except Exception as single_e:
                        print(f"Error inserting individual document (Sequence: {doc.get('sequence')}): {single_e}")
        print(f"{inserted_count} documents insérés dans ArangoDB")
    else:
        print("No new documents to insert into ArangoDB (all were discarded as identical or worse).")
        
    client.close()

if __name__ == '__main__':
    main()
