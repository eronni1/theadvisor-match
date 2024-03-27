from flask import Flask, jsonify
from bson import ObjectId  # This module helps in converting ObjectId to string
from pymongo import MongoClient

app = Flask(__name__)

# Setup MongoDB connection
client = MongoClient('localhost', 27017)
db = client['mag']
collection = db['papers']

# A helper function to convert ObjectId to string
def objectIdToStr(item):
    if isinstance(item, ObjectId):
        return str(item)
    raise TypeError(f"Object of type {item.__class__.__name__} is not JSON serializable")

# Your route to fetch and return papers
@app.route('/papers')
def get_papers():
    papers = list(collection.find().limit(500))  # Fetch 50 papers
    for paper in papers:
        paper['_id'] = objectIdToStr(paper['_id'])  # Convert ObjectId to string
    return jsonify(papers)

if __name__ == '__main__':
    app.run(debug=True)
