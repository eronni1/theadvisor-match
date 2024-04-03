import csv
import os
from pymongo import MongoClient

# MongoDB connection setup
client = MongoClient('mongodb://localhost:27017/')
db = client['mag_dblp']
collection = db['papers']

def insert_paper_to_mongodb(paper):
    try:
        result = collection.insert_one(paper)
        print(f"Inserted paper with ID: {result.inserted_id}")
    except Exception as e:
        print(f"An error occurred while inserting: {e}")

def load_mag_papers(file_path, limit=100):
    with open(file_path, mode='r', encoding='utf-8') as file:
        reader = csv.reader(file)
        next(reader)  # Skip header row
        for i, row in enumerate(reader):
            if i >= limit:
                break
            paper = {
                "line_number": int(row[0]),
                "k": int(row[1]),
                "num_removed_kmers": int(row[2]),
                "mag_id": row[3],
                "best_candidate_paper_dblp_id": row[4],
                "2nd_best_candidate_paper_id": row[5],
                "ratio": row[6],
                "hashmap_build_time": float(row[7]),
                "match": row[8],
                "average_query_time_phase1": float(row[9]),
                "average_query_time_phase2": float(row[10]),
                "average_query_time_total": float(row[11]),
                "levenshteinThreshold": float(row[12]),
                "ratioThreshold": float(row[13]),
                "citation": row[14]
            }
            insert_paper_to_mongodb(paper)

if __name__ == "__main__":
    cwd = os.getcwd()
    file_path = os.path.join(cwd, 'mag_to_dblp_query_total.csv')  # Adjust path as necessary
    collection.drop()  # Clear collection before loading new data
    print("Collection cleared.")

    load_mag_papers(file_path)
    print("Finished loading MAG papers.")
