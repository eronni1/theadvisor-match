import sys
from pymongo import MongoClient
from Parse import parse_MAG_file, Paper

# Setup MongoDB connection
client = MongoClient('localhost', 27017)
db = client['mag']
collection = db['papers']

# Global variable to accumulate papers for batch insertion
paper_batch = []

def insert_into_mongodb(paper):
    global paper_batch  # Reference the global list to accumulate papers
    
    # Convert the Paper object into a dictionary for MongoDB insertion
    paper_dict = paper.__dict__
    paper_batch.append(paper_dict)
    
    # When the batch reaches 100 papers, insert them into MongoDB and clear the batch
    if len(paper_batch) == 100:
        try:
            collection.insert_many(paper_batch)
            print(f"Inserted batch of {len(paper_batch)} papers.")
        except Exception as e:
            print(f"Error inserting batch into MongoDB: {e}")
        paper_batch.clear()  # Clear the batch after insertion

def load_papers_in_batches(file_path):
    # Clear the collection before loading new data
    collection.drop()
    print("Collection cleared.")

    # Callback function to insert papers into MongoDB
    callback = [insert_into_mongodb]

    # Load papers from MAG dataset in batches
    print("Starting to load papers from MAG...")
    parse_MAG_file(callback, 0, 1000)  # Adjust count_to as needed
    print("Finished loading papers from MAG.")

if __name__ == "__main__":
    load_papers_in_batches('mag_to_dblp_query_total.zip')
