# EDA Assistant - Exploratory Data Analysis Platform

A comprehensive data analysis platform built with Streamlit that allows users to upload datasets, manage projects, and perform exploratory data analysis with an AI-powered assistant.

## 🌟 Features

- **User Authentication**: Secure login and registration system with bcrypt password hashing
- **Project Management**: Create and manage multiple data analysis projects
- **Dataset Upload**: Upload CSV files and automatically extract metadata and statistics
- **Knowledge Base**: Attach TXT/PDF documents with domain knowledge to enhance AI analysis
- **AI-Powered Chatbot**: Interactive SQL-based assistant for data exploration
- **Multi-Database Architecture**: PostgreSQL for structured data, MongoDB for knowledge documents

## 📋 Prerequisites

Before you begin, ensure you have the following installed:

- **Python 3.8+** ([Download Python](https://www.python.org/downloads/))
- **PostgreSQL 12+** ([Download PostgreSQL](https://www.postgresql.org/download/))
- **MongoDB 4.4+** ([Download MongoDB](https://www.mongodb.com/try/download/community))






## 🚀 Quick Start

### 1. Clone the Repository
```bash
git clone <your-repository-url>
cd eda-assistant
```

### 2. Set Up Python Virtual Environment (Recommended)
```bash
# Create virtual environment
python -m venv venv

# Activate virtual environment
# On Windows:
venv\Scripts\activate

# On macOS/Linux:
source venv/bin/activate
```

### 3. Install Dependencies
```bash
pip install -r requirements.txt
```

### 4. Configure Databases

#### PostgreSQL Setup
```bash
# Create database (run in terminal)
createdb dbms_project

# If you need to set a password for postgres user:
# On macOS/Linux:
sudo -u postgres psql
ALTER USER postgres PASSWORD 'your_password';
\q

# On Windows (run in psql):
ALTER USER postgres PASSWORD 'your_password';
```

#### MongoDB Setup
MongoDB will create the database automatically. Just ensure the service is running:
```bash
# Check if MongoDB is running
# macOS:
brew services list | grep mongodb

# Linux:
sudo systemctl status mongod

# Windows:
# Check Services app for "MongoDB Server"
```

### 5. Configure Environment Variables (Optional)

If you want to use custom database credentials:
```bash
# Copy the example file
cp .env.example .env

# Edit .env with your credentials
```

Example `.env` file:
```env
DATABASE_URL=postgresql://postgres:your_password@localhost:5432/dbms_project
MONGO_URL=mongodb://localhost:27017
```

### 6. Initialize Database Schema

Run the initialization script to create all required tables:
```bash
python init_db.py
```

You should see:
```
Initializing PostgreSQL...
✅ PostgreSQL schema initialized successfully

Initializing MongoDB...
✅ MongoDB initialized successfully

✅ All databases initialized successfully!
```

### 7. Launch the Application
```bash
streamlit run Dashboard.py
```

The application will open in your default browser at `http://localhost:8501`

## 📁 Project Structure
```
eda-assistant/
│
├── Dashboard.py              # Main Streamlit application
├── init_db.py               # Database initialization script
├── db_config.py             # Centralized database configuration
│
├── Ingestion.py             # Dataset ingestion and metadata extraction
├── Retrieval.py             # Data retrieval and querying functions
├── utils.py                 # Utility functions (auth, projects, etc.)
│
├── mongo_utils.py           # MongoDB operations
├── knowledge_ingestion.py   # Knowledge file processing
│
├── pages/
│   └── Chatbot_sql.py      # AI assistant interface
│
├── requirements.txt         # Python dependencies
├── .env.example            # Environment variables template
└── README.md               # This file
```

## 🔧 Configuration

### Database Configuration

The application uses the following default database settings:

**PostgreSQL:**
- Host: `localhost`
- Port: `5432`
- Database: `dbms_project`
- Username: `postgres`
- Password:  (change in `.env` for production)

**MongoDB:**
- Host: `localhost`
- Port: `27017`
- Database: `eda_assistant`
- Collection: `dataset_knowledge`

### Customizing Database Credentials

1. Create a `.env` file in the project root
2. Add your custom credentials:
```env
   DATABASE_URL=postgresql://username:password@host:port/database
   MONGO_URL=mongodb://host:port
```

## 📊 Database Schema

### PostgreSQL Tables

**user_details**
- User authentication and profile information

**projects**
- Project metadata and ownership

**datasets_metadata**
- Dataset information and statistics

**dataset_column_details**
- Column-level statistics and metadata

**dataset_X_data** (dynamic)
- Actual dataset tables (created per upload)

### MongoDB Collections

**dataset_knowledge**
- Domain knowledge documents linked to projects

## 🎯 Usage

### 1. Register/Login
- Create a new account or login with existing credentials
- All data is isolated per user

### 2. Create a Project
- Navigate to "Create Project"
- Provide a name and optional description

### 3. Upload Dataset
- Go to "Manage Datasets"
- Select a project
- Upload a CSV file
- System automatically extracts:
  - Column statistics (mean, median, std dev)
  - Data types
  - Missing values
  - Unique categories

### 4. Add Knowledge Documents (Optional)
- Upload TXT or PDF files with domain knowledge
- Helps AI assistant provide better context-aware analysis

### 5. Start Analysis
- Click "🤖 Start Assistant" on any project with a dataset
- Ask questions in natural language
- AI generates SQL queries and visualizations

## 🛠️ Troubleshooting

### PostgreSQL Connection Issues
```bash
# Check if PostgreSQL is running
# macOS:
brew services list

# Linux:
sudo systemctl status postgresql

# Windows:
# Check Services app

# Test connection
psql -U postgres -d dbms_project
```

### MongoDB Connection Issues
```bash
# Check if MongoDB is running
# macOS:
brew services list
  
# Linux:
sudo systemctl status mongod

# Test connection
mongosh
```



### Reset Database

If you need to start fresh:
```bash
# Drop and recreate PostgreSQL database
dropdb dbms_project
createdb dbms_project
python init_db.py

# Clear MongoDB collection (optional)
mongosh
use eda_assistant
db.dataset_knowledge.drop()
exit
```



