#mongo_utils.py
from pymongo import MongoClient
from datetime import datetime
from bson import ObjectId
import os


def get_mongo_client():
    """Get MongoDB client with connection from environment or default"""
    mongo_url = os.getenv("MONGO_URL", "mongodb://localhost:27017")
    try:
        client = MongoClient(mongo_url, serverSelectionTimeoutMS=5000)
        # Test connection
        client.admin.command('ping')
        return client
    except Exception as e:
        raise ConnectionError(f"Failed to connect to MongoDB: {e}")


def get_mongo_collection():
    client = get_mongo_client()
    db = client["eda_assistant"]
    return db["dataset_knowledge"]


def init_mongodb():
    """
    Initialize MongoDB collections and indexes.
    MongoDB creates databases/collections automatically, but we set up indexes here.
    Safe to run multiple times.
    """
    try:
        collection = get_mongo_collection()

        # Create indexes for better query performance
        collection.create_index("project_id")
        collection.create_index("dataset_id")
        collection.create_index([("project_id", 1), ("created_at", -1)])

        print("✅ MongoDB initialized successfully")
        return True

    except Exception as e:
        print(f"❌ Error initializing MongoDB: {e}")
        return False


def insert_knowledge_document(
        project_id,
        dataset_id,
        title,
        content,
        source_type
):
    collection = get_mongo_collection()

    doc = {
        "project_id": project_id,
        "dataset_id": dataset_id,
        "title": title,
        "content": content,
        "source_type": source_type,  # "txt" | "pdf" | "manual"
        "created_at": datetime.utcnow()
    }

    result = collection.insert_one(doc)
    return result.inserted_id


def list_project_knowledge_files(project_id):
    """List all knowledge documents for a project"""
    collection = get_mongo_collection()
    docs = collection.find(
        {"project_id": project_id},
        {"title": 1, "source_type": 1, "created_at": 1, "_id": 1}
    )
    return list(docs)


def delete_knowledge_document(doc_id):
    """Delete a specific knowledge document by its ObjectId"""
    collection = get_mongo_collection()

    # Convert string to ObjectId if necessary
    if isinstance(doc_id, str):
        doc_id = ObjectId(doc_id)

    result = collection.delete_one({"_id": doc_id})
    return result.deleted_count > 0


def fetch_project_knowledge_documents(project_id):
    """Fetch all knowledge documents for a project with title and content"""
    collection = get_mongo_collection()
    docs = collection.find({"project_id": project_id})
    return [
        {
            "title": d.get("title", "Untitled"),
            "content": d["content"]
        }
        for d in docs
    ]


if __name__ == "__main__":
    # Test connection and initialize
    init_mongodb()