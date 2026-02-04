#Dashboard.py
import streamlit as st
import pandas as pd
import os

from db_utils.utils import (
    create_new_project,
    upload_dataset_to_project,
    list_projects,
    delete_project,
    get_dataframe,
    get_column_details,
    get_dataset_metadata,
    register_user,
    authenticate_user,
    handle_error

)

from db_utils.mongo_utils import insert_knowledge_document
from db_utils.mongo_utils import list_project_knowledge_files, delete_knowledge_document

from db_utils.knowledge_ingestion import extract_text_from_txt


# Database connection
from db_utils.init_db import init_all_databases
from db_utils.db_config import get_engine


# Ensure required directories exist
REQUIRED_DIRS = ['knowledge', 'uploads', 'logs']
for dir_name in REQUIRED_DIRS:
    os.makedirs(dir_name, exist_ok=True)
# Initialize database tables on first run
if 'db_initialized' not in st.session_state:
    init_all_databases()
    st.session_state.db_initialized = True

# Database connection
@st.cache_resource
def get_db_engine():
    return get_engine()

engine = get_db_engine()

engine = get_engine()

if "authenticated" not in st.session_state:
    st.session_state.authenticated = False

if "user_id" not in st.session_state:
    st.session_state.user_id = None

# Initialize session state
user_id = st.session_state.user_id

if 'refresh' not in st.session_state:
    st.session_state.refresh = 0

if not st.session_state.authenticated:
    st.set_page_config(page_title="Login", layout="centered")
    st.title("🔐 Login")

    tab_login, tab_register = st.tabs(["Login", "Register"])

    with tab_login:
        username = st.text_input("Username")
        password = st.text_input("Password", type="password")

        if st.button("Login", use_container_width=True):

            if not username.strip() and not password:
                st.error("❌ Username and password are required")

            elif not username.strip():
                st.error("❌ Username cannot be empty")

            elif not password:
                st.error("❌ Password cannot be empty")

            else:
                result = authenticate_user(username.strip(), password, engine)

                if result["success"]:
                    st.session_state.authenticated = True
                    st.session_state.user_id = result["user_id"]
                    st.success("Login successful")
                    st.rerun()
                else:
                    st.error(result["error"])

    with tab_register:
        new_username = st.text_input("New Username")
        new_password = st.text_input("New Password", type="password")
        confirm_password = st.text_input("Confirm Password", type="password")

        if st.button("Register", use_container_width=True):

            if not new_username.strip():
                st.error("❌ Username cannot be empty")

            elif not new_password:
                st.error("❌ Password cannot be empty")

            elif not confirm_password:
                st.error("❌ Please confirm your password")

            elif new_password != confirm_password:
                st.error("❌ Passwords do not match")

            else:
                result = register_user(new_username.strip(), new_password, engine)

                if result["success"]:
                    st.success("✅ Account created. Please login.")
                else:
                    st.error(result["error"])

    st.stop()  # ⛔ IMPORTANT: prevents dashboard from loading

user_id = st.session_state.user_id
# Page config
st.set_page_config(
    page_title="Project Manager",
    page_icon="📊",
    layout="wide"
)

# Custom CSS
st.markdown("""
    <style>
    .project-card {
        padding: 1.5rem;
        border-radius: 0.5rem;
        background-color: #f0f2f6;
        margin-bottom: 1rem;
    }
    .success-msg {
        padding: 1rem;
        border-radius: 0.5rem;
        background-color: #d4edda;
        color: #155724;
        margin-bottom: 1rem;
    }
    .error-msg {
        padding: 1rem;
        border-radius: 0.5rem;
        background-color: #f8d7da;
        color: #721c24;
        margin-bottom: 1rem;
    }
    </style>
""", unsafe_allow_html=True)



# Title
st.title("📊 Project Management ")
st.markdown("---")

# Sidebar for user info and navigation
with st.sidebar:
    st.header("🔧 Navigation")
    st.info(f"**User ID:** {st.session_state.user_id}")

    page = st.radio(
        "Select Action",
        ["View Projects", "Create Project", "Manage Datasets"],
        label_visibility="collapsed"
    )
    if st.button("🚪 Logout"):
        st.session_state.authenticated = False
        st.session_state.user_id = None
        st.session_state.messages = []
        st.rerun()

    st.markdown("---")
    if st.button("🔄 Refresh", use_container_width=True):
        st.session_state.refresh += 1
        st.rerun()

# Main content area
if page == "Create Project":
    st.header("➕ Create New Project")

    with st.form("create_project_form"):
        project_name = st.text_input("Project Name*", placeholder="Enter project name")
        description = st.text_area("Description", placeholder="Describe your project (optional)")

        submitted = st.form_submit_button("Create Project", use_container_width=True)

        if submitted:
            if not project_name.strip():
                st.error("❌ Project name is required!")
            else:
                result = create_new_project(
                    user_id=st.session_state.user_id,
                    project_name=project_name,
                    description=description,
                    engine=engine
                )

                if result["success"]:
                    st.success(f"✅ Project created successfully! (ID: {result['project_id']})")
                else:
                    st.error(f"❌ Error: {result['error']}")

elif page == "View Projects":
    st.header("📁 My Projects")

    # Fetch projects
    result = list_projects(st.session_state.user_id, engine)

    if not result.get("success", True):
        st.error(f"❌ Error loading projects: {result.get('error', 'Unknown error')}")
    elif not result.get("project_list"):
        st.info("📝 No projects yet. Create your first project to get started!")
    else:
        projects = result["project_list"]

        # Display summary
        col1, col2 = st.columns(2)
        with col1:
            st.metric("Total Projects", len(projects))
        with col2:
            projects_with_data = sum(1 for p in projects if p['dataset_id'] is not None)
            st.metric("Projects with Datasets", projects_with_data)

        st.markdown("---")

        # Display each project
        for project in projects:
            with st.container():
                col1, col2 = st.columns([4, 1])

                with col1:
                    st.subheader(f"📌 {project['project_name']}")
                    if project['description']:
                        st.write(project['description'])

                    # Project details
                    detail_cols = st.columns(3)
                    with detail_cols[0]:
                        st.caption(f"**Project ID:** {project['project_id']}")
                    with detail_cols[1]:
                        st.caption(f"**Created:** {project['created_at'].strftime('%Y-%m-%d %H:%M')}")
                    with detail_cols[2]:
                        if project['dataset_id']:
                            st.caption(f"**Dataset ID:** {project['dataset_id']}")
                        else:
                            st.caption("**Dataset:** None")

                    # Show dataset info if exists
                    if project['dataset_id']:
                        with st.expander("📊 View Dataset Details"):
                            try:
                                # Get dataset metadata
                                metadata = get_dataset_metadata(project['dataset_id'], engine)
                                if metadata:
                                    st.write(f"**Dataset Name:** {metadata.get('dataset_name', 'N/A')}")
                                    st.write(f"**Filename:** {metadata.get('file_name', 'N/A')}")
                                    st.write(f"**Rows:** {metadata.get('num_rows', 'N/A')}")
                                    st.write(f"**Uploaded:** {metadata.get('upload_date', 'N/A')}")

                                # Get column details
                                columns = get_column_details(project['dataset_id'], engine)
                                if columns:
                                    st.write("**Columns:**")
                                    col_df = pd.DataFrame(columns)
                                    st.dataframe(col_df, use_container_width=True)

                                # Preview data
                                df = get_dataframe(project['dataset_id'], engine, limit=5)
                                if df is not None and not df.empty:
                                    st.write("**Data Preview (first 5 rows):**")
                                    st.dataframe(df, use_container_width=True)
                            except Exception as e:
                                st.error(f"Error loading dataset: {str(e)}")
                        st.markdown("### 📄 Dataset Knowledge Files")



                        # List existing files
                        knowledge_files = list_project_knowledge_files(project['project_id'])

                        if knowledge_files:
                            st.write(f"**Uploaded Files ({len(knowledge_files)}):**")

                            for idx, file_doc in enumerate(knowledge_files):
                                col_file, col_delete = st.columns([5, 1])

                                with col_file:
                                    file_icon = "📄" if file_doc.get('source_type') == 'txt' else "📋"
                                    st.write(f"{file_icon} **{file_doc.get('title', 'Untitled')}**")
                                    if 'created_at' in file_doc:
                                        st.caption(f"Uploaded: {file_doc['created_at'].strftime('%Y-%m-%d %H:%M:%S')}")

                                with col_delete:
                                    if st.button(
                                            "🗑️",
                                            key=f"delete_knowledge_{project['project_id']}_{idx}",
                                            help="Delete this file"
                                    ):
                                        if delete_knowledge_document(file_doc['_id']):
                                            st.success("✅ File deleted!")

                                            # Also clean up the file from knowledge directory
                                            # This will be recreated next time chatbot loads
                                            project_dir = os.path.join("knowledge", f"project_{project['project_id']}")
                                            if os.path.exists(project_dir):
                                                import shutil

                                                try:
                                                    shutil.rmtree(project_dir)
                                                except Exception as e:
                                                    st.warning(f"Could not delete knowledge directory: {str(e)}")

                                            st.rerun()
                                        else:
                                            st.error("❌ Failed to delete")


                            st.markdown("")  # Add some spacing
                        else:
                            st.info("📭 No knowledge files uploaded yet.")

                        # Upload new file section
                        with st.expander("➕ Upload New Knowledge File", expanded=False):
                            knowledge_file = st.file_uploader(
                                "Choose a file",
                                type=["txt", "pdf"],
                                key=f"knowledge_upload_{project['project_id']}",
                                help="Upload TXT or PDF files containing information about your dataset"
                            )

                            if knowledge_file is not None:
                                # Show file preview
                                st.write(f"**Selected:** {knowledge_file.name}")
                                st.write(f"**Size:** {knowledge_file.size / 1024:.2f} KB")

                                if st.button(
                                        "📥 Upload File",
                                        key=f"save_knowledge_{project['project_id']}",
                                        use_container_width=True,
                                        type="primary"
                                ):
                                    with st.spinner("Processing knowledge file..."):
                                        try:
                                            file_bytes = knowledge_file.getvalue()
                                            filename = knowledge_file.name.lower()

                                            if filename.endswith(".txt"):
                                                content = extract_text_from_txt(file_bytes)
                                                source_type = "txt"
                                            elif filename.endswith(".pdf"):
                                                # Add PDF support if you have it
                                                st.error("PDF support coming soon!")
                                                st.stop()
                                            else:
                                                st.error("Unsupported file type")
                                                st.stop()

                                            # Use original filename (no timestamp needed - MongoDB _id is unique)
                                            insert_knowledge_document(
                                                project_id=project["project_id"],
                                                dataset_id=project["dataset_id"],
                                                title=knowledge_file.name,
                                                content=content,
                                                source_type=source_type
                                            )

                                            st.success("✅ Knowledge file uploaded successfully!")
                                            st.balloons()
                                            st.rerun()

                                        except Exception as e:
                                            st.error(f"❌ Error uploading file: {str(e)}")


                with col2:

                    # In Dashboard.py - Replace the Start Assistant button code:

                    if project["dataset_id"] is not None:
                        if st.button(
                                "🤖 Start Assistant",
                                key=f"assistant_{project['project_id']}",
                                use_container_width=True
                        ):
                            st.session_state.project_id = project['project_id']
                            # Make sure user_id is always set
                            if "user_id" not in st.session_state:
                                st.session_state.user_id = user_id  # Make sure to set this too
                            # Use query params to pass project_id

                            st.switch_page("pages/Chatbot_sql.py")
                    else:
                        st.button(
                            "🚫 No Dataset",
                            key=f"assistant_disabled_{project['project_id']}",
                            disabled=True,
                            use_container_width=True
                        )

                    # Delete button
                    if st.button("🗑️ Delete", key=f"del_{project['project_id']}", use_container_width=True):
                        st.session_state[f"confirm_delete_{project['project_id']}"] = True

                    # Confirmation dialog
                    if st.session_state.get(f"confirm_delete_{project['project_id']}", False):
                        st.warning("⚠️ Confirm?")
                        col_a, col_b = st.columns(2)
                        with col_a:
                            if st.button("Yes", key=f"yes_{project['project_id']}", use_container_width=True):
                                delete_result = delete_project(
                                    project['project_id'],
                                    st.session_state.user_id,
                                    engine
                                )
                                if delete_result["success"]:
                                    st.success("✅ Deleted!")
                                    st.session_state[f"confirm_delete_{project['project_id']}"] = False
                                    st.rerun()
                                else:
                                    st.error(f"❌ {delete_result['error']}")
                        with col_b:
                            if st.button("No", key=f"no_{project['project_id']}", use_container_width=True):
                                st.session_state[f"confirm_delete_{project['project_id']}"] = False
                                st.rerun()

                st.markdown("---")

elif page == "Manage Datasets":
    st.header("📂 Upload Dataset to Project")

    # Fetch projects
    result = list_projects(st.session_state.user_id, engine)

    if not result.get("success", True):
        st.error(f"❌ Error loading projects: {result.get('error', 'Unknown error')}")
    elif not result.get("project_list"):
        st.warning("⚠️ No projects available. Create a project first!")
    else:
        projects = result["project_list"]

        # Filter projects without datasets
        available_projects = [p for p in projects if p['dataset_id'] is None]

        if not available_projects:
            st.info("ℹ️ All your projects already have datasets. Create a new project or delete an existing dataset.")
        else:
            # Select project
            project_options = {f"{p['project_name']} (ID: {p['project_id']})": p['project_id']
                               for p in available_projects}

            selected_project = st.selectbox(
                "Select Project",
                options=list(project_options.keys())
            )

            if selected_project:
                project_id = project_options[selected_project]

                st.markdown("---")

                # File uploader
                uploaded_file = st.file_uploader(
                    "Choose a CSV file",
                    type=['csv'],
                    help="Upload a CSV file to attach to this project"
                )

                if uploaded_file is not None:
                    # Preview uploaded file
                    st.subheader("📋 Data Preview")
                    try:
                        preview_df = pd.read_csv(uploaded_file, nrows=5)
                        st.dataframe(preview_df, use_container_width=True)

                        # Show file info
                        uploaded_file.seek(0)
                        file_details = {
                            "Filename": uploaded_file.name,
                            "File size": f"{uploaded_file.size / 1024:.2f} KB",
                            "Columns": len(preview_df.columns),
                        }
                        st.json(file_details)

                        # Upload button
                        if st.button("📤 Upload Dataset", use_container_width=True, type="primary"):
                            with st.spinner("Uploading dataset..."):
                                # Save file temporarily
                                import tempfile
                                import os

                                with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as tmp_file:
                                    uploaded_file.seek(0)
                                    tmp_file.write(uploaded_file.getvalue())
                                    tmp_path = tmp_file.name

                                try:
                                    # Upload to database
                                    upload_result = upload_dataset_to_project(
                                        project_id=project_id,
                                        csv_path=tmp_path,
                                        original_filename=uploaded_file.name,
                                        user_id=st.session_state.user_id,
                                        engine=engine
                                    )

                                    if upload_result["success"]:
                                        st.success(
                                            f"✅ Dataset uploaded successfully! (Dataset ID: {upload_result['dataset_id']})")
                                    else:
                                        st.error(f"❌ Error: {upload_result['error']}")
                                finally:
                                    # Clean up temp file
                                    if os.path.exists(tmp_path):
                                        os.unlink(tmp_path)

                    except Exception as e:
                        st.error(f"❌ Error reading file: {str(e)}")

# Footer
st.markdown("---")
st.caption("💡 Tip: Use the sidebar to navigate between different actions")

try:
    pass
except Exception:
    handle_error("An unexpected error occurred.")