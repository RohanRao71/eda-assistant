import streamlit as st
from crewai import Agent, Task, Crew, LLM
from crewai.knowledge.source.text_file_knowledge_source import TextFileKnowledgeSource
import json
from sqlalchemy import create_engine, text
from db_utils.utils import (
    get_project_metadata,
    get_project_stats
)

import re
import pandas as pd

from db_utils.mongo_utils import get_mongo_collection

import os
from db_utils.mongo_utils import fetch_project_knowledge_documents

os.environ["EMBEDDINGS_OLLAMA_MODEL_NAME"] = "mxbai-embed-large:latest"


def build_knowledge_source_from_documents(documents_list, embedder, project_id):
    """
    Save documents to knowledge/ directory and create knowledge source
    documents_list: list of dicts with 'title' and 'content'
    """
    if not documents_list:
        return None

    # Create knowledge directory if it doesn't exist
    knowledge_dir = "knowledge"
    os.makedirs(knowledge_dir, exist_ok=True)

    # Create project-specific subdirectory
    project_dir = os.path.join(knowledge_dir, f"project_{project_id}")
    os.makedirs(project_dir, exist_ok=True)

    file_paths = []

    for i, doc in enumerate(documents_list):
        # Create a safe filename from the title
        safe_title = "".join(c if c.isalnum() or c in (' ', '_', '-') else '_' for c in doc['title'])
        safe_title = safe_title.strip().replace(' ', '_')[:50]  # Limit length

        # Use index to ensure uniqueness
        file_path = os.path.join(project_dir, f"{i}_{safe_title}.txt")

        # Write content to file
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(f"# {doc['title']}\n\n")  # Add title as header
            f.write(doc['content'])

        file_paths.append(f"project_{project_id}/{i}_{safe_title}.txt")


    if file_paths:
        return TextFileKnowledgeSource(
            file_paths=file_paths,
            embedder=embedder
        )
    return None


def cleanup_project_knowledge_files(project_id):
    """
    Remove knowledge files for a project
    """
    import shutil
    project_dir = os.path.join("knowledge", f"project_{project_id}")
    if os.path.exists(project_dir):
        shutil.rmtree(project_dir)


def fetch_project_knowledge(project_id):
    coll = get_mongo_collection()
    docs = coll.find({"project_id": project_id})
    return "\n\n".join(d["content"] for d in docs)


def sort_columns_for_replacement(columns):
    return sorted(columns, key=len, reverse=True)


def quote_identifiers_in_sql(sql, allowed_columns):
    for col in sort_columns_for_replacement(allowed_columns):
        pattern = rf'(?<!")\b{re.escape(col)}\b(?!")'
        sql = re.sub(pattern, f'"{col}"', sql)
    return sql


def extract_limit(sql, default=30):
    match = re.search(r'\bLIMIT\s+(\d+)', sql, re.IGNORECASE)
    return int(match.group(1)) if match else default


def clamp_limit(limit, max_limit=50):
    return min(limit, max_limit)


def build_safe_select(table_name, allowed_columns, limit):
    cols = ", ".join(f'"{c}"' for c in allowed_columns)
    return f'SELECT {cols} FROM {table_name} LIMIT {limit}'


def validate_sql(sql, table_name, allowed_columns):
    try:
        sql_upper = sql.upper().strip()
        # Normalize quoted table name → unquoted
        sql = re.sub(
            rf'\bFROM\s+"{re.escape(table_name)}"\b',
            f'FROM {table_name}',
            sql,
            flags=re.IGNORECASE
        )

        if not sql_upper.startswith("SELECT"):
            raise ValueError("Not a SELECT")

        # Hard forbid quoted star only
        if re.search(r'"\*"', sql):
            raise ValueError('Quoted "*" is not allowed')

        forbidden = ["INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "TRUNCATE"]
        for kw in forbidden:
            if kw in sql_upper:
                raise ValueError(f"Forbidden keyword: {kw}")

        # Clamp LIMIT
        limit = min(extract_limit(sql, 30), 50)

        # Validate table name
        table_pattern = rf'\bFROM\s+{re.escape(table_name)}\b'

        if not re.search(table_pattern, sql, re.IGNORECASE):
            raise ValueError("Wrong table")

        # 🔑 REMOVE FROM clause before column validation
        sql_without_from = re.sub(
            table_pattern,
            '',
            sql,
            flags=re.IGNORECASE
        )

        # Validate quoted identifiers as columns ONLY
        for col in re.findall(r'"([^"]+)"', sql_without_from):
            if col not in allowed_columns:
                raise ValueError(f"Invalid column: {col}")

        # Normalize LIMIT
        sql = re.sub(r'\bLIMIT\s+\d+', f'LIMIT {limit}', sql, flags=re.IGNORECASE)
        if not re.search(r'\bLIMIT\b', sql, re.IGNORECASE):
            sql = f"{sql} LIMIT {limit}"

        return sql

    except Exception:
        return build_safe_select(
            table_name=table_name,
            allowed_columns=allowed_columns,
            limit=min(extract_limit(sql, 30), 50)
        )




st.set_page_config(
    page_title="EDA Assistant Chatbot",
    page_icon="📊",
    layout="wide"
)


@st.cache_resource
def get_engine():
    return create_engine("postgresql://postgres:5617@localhost:5432/dbms_project")


engine = get_engine()

# -------------------------
# Initialize Session State
# -------------------------
if 'messages' not in st.session_state:
    st.session_state.messages = []

# CHECK FOR PROJECT CONTEXT FIRST
if "project_id" not in st.session_state or "user_id" not in st.session_state:
    st.error("🚫 No project selected")
    st.info("Please select a project from the dashboard to start the assistant.")

    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        if st.button("⬅️ Go to Dashboard", use_container_width=True, type="primary"):
            st.switch_page("../Dashboard.py")
    st.stop()

project_id = st.session_state.project_id
user_id = st.session_state.user_id

# INITIALIZE OR RE-INITIALIZE CREWS
if (
        "crews_initialized" not in st.session_state
        or st.session_state.get("crews_project_id") != project_id
):
    # Clear messages when switching projects
    st.session_state.messages = []
    if "crews_project_id" in st.session_state and st.session_state.crews_project_id != project_id:
        cleanup_project_knowledge_files(st.session_state.crews_project_id)
    st.session_state.crews_project_id = project_id

    # LLM Setup
    llm = LLM(
        model="ollama/llama3.2:latest",
        temperature=0.2
    )


    llm_sql = LLM(
        model = "ollama/duckdb-nsql:7b",
        temperature=0
    )

    embedder = {
        "provider": "ollama",
        "config": {
            "model": "mxbai-embed-large:latest"
        }
    }

    project_meta = get_project_metadata(project_id=project_id, engine=engine)
    dataset = project_meta["project"]["dataset"]

    if dataset is None:
        st.error("This project has no dataset. Please upload a dataset first.")
        st.stop()

    metadata = dataset
    metadata.pop("upload_date", None)

    stats = get_project_stats(project_id=project_id, engine=engine)
    stats = stats["stats"]

    column_names = metadata["column_names"]
    table_name = metadata["table_name"]
    allowed_columns = set(column_names)

    # Fetch knowledge documents from MongoDB
    project_knowledge_docs = fetch_project_knowledge_documents(project_id)

    if project_knowledge_docs:
        # Save to knowledge/ directory and create knowledge source
        dataset_knowledge_source = build_knowledge_source_from_documents(
            project_knowledge_docs,
            embedder=embedder,
            project_id=project_id
        )
    else:
        dataset_knowledge_source = None

    # Define agents
    routing_agent = Agent(
        role="Route Determination Agent",
        goal="Determining the route taken to answer user's question",
        backstory="You look at a user's question about a dataset and determine whether "
                  "data retrieval is needed or the question can be answered using knowledge sources.",
        llm=llm,
        verbose=False
    )

    knowledge_agent = Agent(
        role="Answering Agent",
        goal="Answer user's question about a dataset using given knowledge sources",
        backstory="You are an expert at knowing how to answer a user's EDA related question for dataset. "
                  "You use the given knowledge sources to the best of your abilities",
        llm=llm,
        verbose=False,
        knowledge_sources=[dataset_knowledge_source] if dataset_knowledge_source else [],
        embedder=embedder
    )

    sql_agent = Agent(
        role="SQL Query Generator",
        goal="Generate a SQL SELECT query to retrieve data from a dataset",
        backstory="You translate user questions into safe, read-only SQL queries. "
                  "You never modify data and never invent schema.",
        llm=llm,
        verbose=False
    )

    analysis_agent = Agent(
        role="Data Analysis Agent",
        goal="Analyze data from a dataset and answer user prompts",
        backstory="You are the last step in an EDA assistant agentic workflow. "
                  "You are an expert EDA agent capable at looking at samples of data from a dataset and obtaining "
                  "important insights relevant to a user's question as well as plans for the next EDA steps.",
        llm=llm,
        verbose=False
    )

    # Define tasks
    routing_task = Task(
        description="Look at the user's prompt - {prompt}, for the dataset - {dataset}. "
                    "If the prompt is related to column level stats or dataset metadata "
                    "and can be answered with the help of the knowledge sources: "
                    "Dataset Metadata - {metadata}, or Column-level stats - {stats} "
                    "then output just one word - 'Knowledge'. "
                    "If the user specifically asks for some kind of data retrieval "
                    "or data needs to be fetched from the dataset then output just one word - 'Data'. "
                    """Decision rules:
                        - If the question asks to show examples, samples, specific records,
                          trends visible only in raw rows, or requests to "look at data",
                          output: Data
                        - If the question asks for number of rows/columns, dataset metadata,
                          schema, distributions, counts, averages, missing values, or general properties, output: Knowledge""",
        expected_output="One word - 'Data' or 'Knowledge' depending on route.",
        agent=routing_agent
    )

    knowledge_task = Task(
        description="You are an agent in an EDA assistant workflow. "
                    "Look at the user's prompt - {prompt}, for the dataset - {dataset}. "
                    "Using the dataset metadata - {metadata} and column-level statistics - {stats}, "
                    "answer the user's question. You also have a knowledge source containing a dataset description "
                    "for answering questions. "
                    "Next provide 3-4 relevant insights and the next steps for EDA "
                    "based on the user prompt. "
                    "Do not make up your own numbers or stats. Answer based only on knowledge sources given.",
        expected_output="A relevant and accurate answer to the user's question and insights on the information.",
        agent=knowledge_agent
    )

    sql_task = Task(
        description="""
    Generate a SINGLE PostgreSQL SELECT query to answer the user's request.

    User question:
    "{prompt}"

    Dataset name:
    "{dataset}"

    Dataset table:
    {table_name}

    Available columns (exact names):
    {column_names}

    STRICT RULES:
    - Output ONLY a single SQL SELECT query.
    - Do NOT keep the table name in double quotes.
    - Do NOT include explanations, comments, or markdown.
    - Do NOT use INSERT, UPDATE, DELETE, DROP, ALTER.
    - Do NOT reference any table other than {table_name}.
    - Do NOT invent column names. 
    - Column names MUST be in double quotes.
    - Use LIMIT if the user specifies a number of rows.
    - If no limit is specified, default to LIMIT 30.
    """,
        expected_output="A single valid PostgreSQL SELECT query.",
        agent=sql_agent
    )

    analysis_task = Task(
        description=(
            "You are given:\n"
            "- A user question: {prompt}\n"
            "- A dataset name: {dataset}\n"
            "- A JSON-formatted data which is a sample of the dataset: {data}\n\n"

            "Rules (MUST FOLLOW STRICTLY):\n"
            "1. You may ONLY use the rows, columns, and values explicitly present in {data}.\n"
            "2. Do NOT infer, estimate, assume, or compute any statistic that cannot be directly derived "
            "from the provided data.\n"
            "3. Do NOT mention any column that does not appear in {data}.\n"
            "4. Do NOT invent totals, averages, distributions, trends, or correlations beyond what is "
            "directly observable in the given rows.\n"

            "Response format (EXACT ORDER):\n"
            "Section 1 — Data Preview:\n"
            "- Render ONLY the provided json data in a markdown table in the exact same structure."
            " Answer the user's question\n\n"

            "Section 2 — Observations:\n"
            "- List a few factual observations that are strictly visible in the data.\n\n"

            "Section 3 — Next EDA Steps:\n"
            "- Suggest 2-3 analysis steps that would require MORE data or aggregation, "
            "without performing those analyses.\n"
        ),
        expected_output=(
            "A response containing exactly four sections:\n"
            "Data Preview, Direct Answer, Observations, Next EDA Steps."
        ),
        agent=analysis_agent
    )

    # Create crews
    st.session_state.routing_crew = Crew(agents=[routing_agent], tasks=[routing_task])
    st.session_state.knowledge_crew = Crew(agents=[knowledge_agent], tasks=[knowledge_task])
    st.session_state.data_crew = Crew(agents=[sql_agent], tasks=[sql_task])
    st.session_state.analysis_crew = Crew(agents=[analysis_agent], tasks=[analysis_task])

    # Store metadata
    st.session_state.metadata = metadata
    st.session_state.stats = stats
    st.session_state.project_id = project_id
    st.session_state.column_names = column_names
    st.session_state.table_name = table_name
    st.session_state.allowed_columns = allowed_columns
    st.session_state.crews_initialized = True

# -------------------------
# UI SECTION (ALWAYS RUNS)
# -------------------------
st.title("📊 EDA Assistant Chatbot")
st.markdown(f"Ask questions about your **{st.session_state.metadata.get('dataset_name', 'Dataset')}** dataset")

# Sidebar with dataset info
with st.sidebar:
    if st.button("⬅ Back to Dashboard"):
        st.switch_page(r"C:\Users\raoro\PycharmProjects\Data Exploration Assistant - DBMS\Dashboard.py")

    st.header("Dataset Information")
    st.write(f"**Columns:** {len(st.session_state.metadata.get('column_names', []))}")
    st.write(f"**Project ID:** {st.session_state.project_id}")

    if st.button("Clear Chat History"):
        st.session_state.messages = []
        st.rerun()

    with st.expander("View Available Columns"):
        for col in st.session_state.metadata.get('column_names', []):
            st.write(f"- {col}")

# Display chat history
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])

# Chat input
if prompt := st.chat_input("Ask about your dataset..."):
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        with st.spinner("Analyzing your question..."):
            try:
                routing_output = st.session_state.routing_crew.kickoff(
                    inputs={
                        "prompt": prompt,
                        "dataset": st.session_state.metadata.get('dataset_name', 'Dataset'),
                        "metadata": st.session_state.metadata,
                        "stats": st.session_state.stats
                    }
                )

                cleaned_output = str(routing_output).strip()

                if "Knowledge" in cleaned_output:
                    with st.status("Using knowledge sources...", expanded=True):
                        st.write("📚 Accessing metadata and statistics")
                        knowledge_output = st.session_state.knowledge_crew.kickoff(
                            inputs={
                                "prompt": prompt,
                                "dataset": st.session_state.metadata.get('dataset_name', 'Dataset'),
                                "metadata": st.session_state.metadata,
                                "stats": st.session_state.stats
                            }
                        )
                        response = str(knowledge_output)

                else:  # Data route
                    with st.status("Retrieving and analyzing data...", expanded=True):
                        st.write("🔍 Planning data retrieval")
                        data_output = st.session_state.data_crew.kickoff(
                            inputs={
                                "prompt": prompt,
                                "dataset": st.session_state.metadata.get('dataset_name', 'Dataset'),
                                "column_names": st.session_state.column_names,
                                "table_name": st.session_state.table_name
                            }
                        )
                        sql_query = str(data_output).strip()
                        print(sql_query)
                        print(st.session_state.column_names)
                        safe_sql = quote_identifiers_in_sql(sql_query, st.session_state.allowed_columns)
                        final_sql = validate_sql(
                            safe_sql,
                            table_name=st.session_state.table_name,
                            allowed_columns=st.session_state.allowed_columns
                        )
                        print(final_sql)

                        try:
                            with engine.connect() as conn:
                                df = pd.read_sql(text(final_sql), conn)

                            json_data = df.head(100).to_dict(orient="records")
                            json_text = json.dumps(json_data, indent=2)
                            print(json_data)
                            st.write("🤖 Analyzing data")
                            analysis_output = st.session_state.analysis_crew.kickoff(
                                inputs={
                                    "prompt": prompt,
                                    "dataset": st.session_state.metadata.get('dataset_name', 'Dataset'),
                                    "data": json_text
                                }
                            )
                            response = str(analysis_output)

                        except Exception as e:
                            response = f"❌ An error occurred while retrieving data: {str(e)}"

                st.markdown(response)

            except Exception as e:
                response = f"❌ An error occurred: {str(e)}"
                st.error(response)

    st.session_state.messages.append({"role": "assistant", "content": response})

# Footer
st.markdown("---")
st.markdown(
    "💡 **Tip:** You can ask about column statistics, request data samples, or explore patterns in your dataset!")