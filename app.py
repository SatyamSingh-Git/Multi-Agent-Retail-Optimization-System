# streamlit_app.py
# --- Required Imports ---
import streamlit as st
import pandas as pd
import os
import subprocess
import sys
import glob
import datetime
from io import StringIO
import traceback # For detailed error reporting

# --- Configuration ---
# Define base directory assuming streamlit_app.py is in the project root
APP_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(APP_DIR, "data")
DATABASE_DIR = os.path.join(APP_DIR, "database")
DB_SETUP_SCRIPT = os.path.join(DATABASE_DIR, "database_setup.py")
DB_FILE = os.path.join(DATABASE_DIR, "retail_data.db")
MAIN_SCRIPT = os.path.join(APP_DIR, "main.py") # Assuming main.py is also in root

# Define specific CSV filenames
INVENTORY_CSV = "inventory_monitoring.csv"
DEMAND_CSV = "demand_forcast.csv"
PRICING_CSV = "pricing_optimization.csv"

EXPECTED_CSVS = {
    "Demand Forecast": DEMAND_CSV,
    "Inventory Monitoring": INVENTORY_CSV,
    "Pricing Optimization": PRICING_CSV
}
REPORT_FILENAME_PATTERN = os.path.join(APP_DIR, "Agent_Workflow_Report_*.docx") # Pattern in app dir

# --- Helper Functions ---

@st.cache_data(ttl=3600) # Cache data for an hour unless cleared
def load_csv_data(filename):
    """Loads data, cleans columns, ensures numeric IDs, drops bad rows."""
    filepath = os.path.join(DATA_DIR, filename)
    if not os.path.exists(filepath):
        # Don't display error here, handled by caller
        return None
    try:
        df = pd.read_csv(filepath)
        if df.empty:
             # Return empty DataFrame, not None, handled by caller
             return pd.DataFrame()

        # --- Data Cleaning ---
        df.columns = df.columns.str.strip()
        # Identify potential ID column names (handle space variation)
        pid_col = 'ProductID' if 'ProductID' in df.columns else 'Product ID'
        sid_col = 'StoreID' if 'StoreID' in df.columns else 'Store ID'
        required_cols = []
        if pid_col in df.columns: required_cols.append(pid_col)
        if sid_col in df.columns: required_cols.append(sid_col)

        # Check if essential ID columns are present
        if not all(col in df.columns for col in required_cols):
            st.error(f"Missing critical ID columns (ProductID/Product ID or StoreID/Store ID) in {filename}. Found: {df.columns.tolist()}")
            return None # Cannot proceed without essential IDs

        # Convert ID columns to numeric, coercing errors to NaN
        for col in required_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')

        # Drop rows where essential IDs are NaN AFTER coercion
        original_rows = len(df)
        df.dropna(subset=required_cols, inplace=True)
        dropped_rows = original_rows - len(df)
        # Optional info message if rows were dropped
        # if dropped_rows > 0:
        #    st.info(f"Note: Dropped {dropped_rows} rows from {filename} due to invalid/missing IDs.")

        if df.empty:
            # File had rows initially but none were valid after cleaning
            return pd.DataFrame()

        # Ensure ID columns are integer type for consistent comparison
        try:
            for col in required_cols:
                df[col] = df[col].astype(int)
        except ValueError as e:
            st.error(f"Could not convert ID columns {required_cols} to integer in {filename}. Check data. Error: {e}")
            return None # Failed integer conversion is critical

        return df
    except Exception as e:
        st.error(f"An error occurred loading or processing {filename}:")
        st.exception(e) # Display full error in Streamlit app
        return None

def save_csv_data(df, filename):
    """Saves a DataFrame back to a CSV file in the data directory."""
    filepath = os.path.join(DATA_DIR, filename)
    try:
        os.makedirs(DATA_DIR, exist_ok=True)
        df.to_csv(filepath, index=False)
        return True
    except Exception as e:
        st.error(f"Error saving {filename}:")
        st.exception(e)
        return False

def save_uploaded_file(uploaded_file_obj, filename):
    """Saves an uploaded file object to the data directory."""
    filepath = os.path.join(DATA_DIR, filename)
    try:
        os.makedirs(DATA_DIR, exist_ok=True)
        with open(filepath, "wb") as f:
            f.write(uploaded_file_obj.getbuffer())
        return True
    except Exception as e:
        st.error(f"Error saving uploaded {filename}: {e}")
        return False

def run_script(script_path, args=[]):
    """Runs a python script using subprocess and yields output lines."""
    python_executable = sys.executable # Use the same python interpreter running Streamlit
    command = [python_executable, script_path] + args
    st.info(f"⚙️ Running command: `{' '.join(command)}`")
    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        encoding='utf-8',
        errors='replace', # Handle potential encoding errors in script output
        bufsize=1, # Line buffered output
        cwd=APP_DIR # Run script from the app's directory
    )
    try:
        # Read and yield stdout
        if process.stdout:
            for line in iter(process.stdout.readline, ''):
                yield line.strip()
            process.stdout.close()

        # Capture stderr
        stderr_output = ""
        if process.stderr:
            stderr_output = process.stderr.read()
            process.stderr.close()

        process.wait() # Wait for process termination

        # Check return code and yield errors if any
        if process.returncode != 0:
            yield f"\n--- ⚠️ ERROR ---"
            yield f"Script exited with error code: {process.returncode}"
            yield f"--- Stderr Output ---"
            yield stderr_output.strip()
        else:
            yield f"\n--- ✅ Script finished successfully ---"
    except Exception as e:
        yield f"\n--- 💥 SUBPROCESS EXECUTION ERROR ---"
        yield f"Error trying to run script {script_path}: {e}"
        yield traceback.format_exc()
    finally:
         # Ensure process is terminated if still running (though wait() should handle)
        if process.poll() is None:
            process.terminate()


def find_latest_report(pattern=REPORT_FILENAME_PATTERN):
    """Finds the most recently created report file matching the pattern in the app directory."""
    try:
        # Ensure the pattern searches in the correct directory
        search_pattern = os.path.join(APP_DIR, os.path.basename(pattern))
        list_of_files = glob.glob(search_pattern)
        if not list_of_files:
            return None
        # Get full paths and find the latest based on modification time
        latest_file = max(list_of_files, key=os.path.getmtime)
        return latest_file
    except Exception as e:
        st.error(f"Error finding report files: {e}")
        return None

def find_record_index(df, product_id_str, store_id_str):
    """Finds index, handling potential column names and ensures numeric comparison."""
    if df is None or df.empty:
        # st.info("Inventory data frame is not loaded or is empty.") # Called frequently, minimize info message
        return None

    pid_col = 'ProductID' if 'ProductID' in df.columns else 'Product ID'
    sid_col = 'StoreID' if 'StoreID' in df.columns else 'Store ID'

    if pid_col not in df.columns or sid_col not in df.columns:
        st.error(f"Inventory DataFrame missing required ID columns ('{pid_col}' or '{sid_col}' expected).")
        return None

    try:
        product_id_int = int(product_id_str)
        store_id_int = int(store_id_str)
        # Comparison assumes df ID columns are already integers from load_csv_data
        idx = df.index[
            (df[pid_col] == product_id_int) &
            (df[sid_col] == store_id_int)
        ].tolist()
        return idx[0] if idx else None
    except ValueError:
        st.error("ProductID and StoreID input must be valid numbers.")
        return None
    except Exception as e:
        st.error(f"Error during record search:")
        st.exception(e)
        return None

# --- Streamlit App Layout ---
st.set_page_config(layout="wide", page_title="MAROS Console")

# --- Header ---
st.image("https://img.icons8.com/external-flaticons-lineal-color-flat-icons/64/external-multi-agent-system-artificial-intelligence-flaticons-lineal-color-flat-icons.png", width=64)
st.title("🤖 MAROS - Multi-Agent Retail Optimization System")
st.caption("An intelligent console for optimizing inventory through collaborative AI agents.")

# --- Sidebar ---
with st.sidebar:
    st.header("About MAROS")
    st.info(
        """
        MAROS leverages specialized AI agents to enhance retail operations:
        - **Forecaster:** Predicts demand.
        - **Monitor:** Tracks stock levels & expiry.
        - **Replenisher:** Calculates optimal orders.
        - **Pricer:** Suggests dynamic pricing.
        - **Coordinator:** Manages workflow.
        """
    )
    st.markdown("---")
    st.header("Navigation")
    st.markdown("Use the tabs above to manage data, edit records, run the workflow, and view reports.")
    st.markdown("---")
    st.button("🔄 Refresh App State", on_click=st.rerun, use_container_width=True)


# --- Main Content Tabs ---
tab1, tab2, tab3, tab4 = st.tabs([
    "📊 **Data Management**", "✏️ **Edit Inventory Record**",
    "🚀 **Run MAROS Workflow**", "📋 **View Reports**"
])

# --- Tab 1: Data Management ---
with tab1:
    try:
        st.header("View & Update Datasets")
        st.markdown("View current data files or upload new CSVs to update the system's inputs.")
        st.divider()

        # --- View Section ---
        col1_t1, col2_t1 = st.columns(2)
        df_view_t1 = None
        with col1_t1:
            st.subheader("📁 View Current Data")
            selected_file_key_t1 = st.selectbox("Select Dataset:", options=[""] + list(EXPECTED_CSVS.keys()), label_visibility="collapsed", key="data_view_select_t1")
            if selected_file_key_t1:
                selected_filename_t1 = EXPECTED_CSVS.get(selected_file_key_t1)
                if selected_filename_t1: # Ensure a valid selection
                    df_view_t1 = load_csv_data(selected_filename_t1)
                    if df_view_t1 is not None and not df_view_t1.empty: st.success(f"Displaying: `{selected_filename_t1}`")
                    elif df_view_t1 is not None: st.warning(f"`{selected_filename_t1}` loaded but is empty or has no valid data.")
                    else: st.warning(f"Could not load or find file `{selected_filename_t1}` in `{DATA_DIR}/`.")
                else: selected_file_key_t1 = None # Reset if selection somehow invalid
            else: st.info("Select a dataset to view.")
        with col2_t1:
            if df_view_t1 is not None and not df_view_t1.empty:
                st.dataframe(df_view_t1, height=350, use_container_width=True)
            elif selected_file_key_t1: st.markdown("*(No data to display)*")

        st.divider()

        # --- Upload Section (Using Session State Staging) ---
        st.subheader("⬆️ Upload & Apply New Data Files")
        st.caption(f"Select files to replace existing ones in `{DATA_DIR}`. Click 'Apply Uploads' to save.")
        if 'staged_files' not in st.session_state: st.session_state.staged_files = {}

        upload_cols_t1 = st.columns(len(EXPECTED_CSVS))
        files_staged_t1 = False
        for i, (key, filename) in enumerate(EXPECTED_CSVS.items()):
            with upload_cols_t1[i]:
                 staged_info = ""
                 if filename in st.session_state.staged_files and st.session_state.staged_files[filename] is not None:
                     try: staged_info = f"*(Staged: {st.session_state.staged_files[filename].name})*"; files_staged_t1 = True
                     except Exception: staged_info = "*(File Staged)*"; files_staged_t1 = True # Handle potential stale file object issues

                 uploaded_file_t1 = st.file_uploader(f"Upload {key} {staged_info}", type="csv", key=f"upload_{filename}_stage_t1")
                 if uploaded_file_t1 is not None:
                     st.session_state.staged_files[filename] = uploaded_file_t1
                     files_staged_t1 = True # Ensure flag is set if new file uploaded

        if st.button("✅ Apply Staged Uploads", disabled=not files_staged_t1, key="apply_uploads_t1"):
            saved_count_t1 = 0
            with st.spinner("Saving uploaded files..."):
                for filename, file_obj in list(st.session_state.staged_files.items()): # Iterate copy of items
                    if file_obj is not None:
                        if save_uploaded_file(file_obj, filename):
                            st.success(f"`{filename}` saved successfully.")
                            saved_count_t1 += 1
                        else: st.error(f"Failed to save `{filename}`.")
                        # Remove processed file from session state regardless of success
                        st.session_state.staged_files[filename] = None
            # Clear the dict only if needed, but setting values to None is safer
            # st.session_state.staged_files = {}

            if saved_count_t1 > 0:
                st.info("Clearing data cache and refreshing app...")
                load_csv_data.clear() # Clear cache ONCE
                st.rerun() # Rerun ONCE after all saves
            else:
                st.warning("No files were staged or saved.")

        st.divider()
        # --- DB Reset Section ---
        with st.expander("⚠️ Advanced Database Operations"):
             st.warning("This deletes the current database and rebuilds the schema from scratch.")
             if st.button("🔄 Reset Database & Re-Initialize Schema", key="reset_db_t1"):
                 placeholder_db_t1 = st.empty()
                 log_output_t1 = f"Attempting to delete {DB_FILE}...\n"
                 try:
                     if os.path.exists(DB_FILE): os.remove(DB_FILE); log_output_t1 += "Deleted existing database.\n"
                     else: log_output_t1 += "Database file not found (Clean start).\n"

                     log_output_t1 += f"Running {DB_SETUP_SCRIPT}...\n"
                     placeholder_db_t1.code(log_output_t1, language="bash")
                     with st.spinner("Initializing database schema..."):
                         for line in run_script(DB_SETUP_SCRIPT):
                             log_output_t1 += line + "\n"
                             placeholder_db_t1.code(log_output_t1, language="bash")
                     if "error" not in log_output_t1.lower(): st.success("Database reset and schema initialized successfully.")
                     else: st.error("Database setup script finished with errors.")
                 except Exception as e:
                     st.error(f"Error during database reset: {e}")
                     log_output_t1 += f"\nERROR: {e}\n{traceback.format_exc()}"
                     placeholder_db_t1.code(log_output_t1, language="bash")
    except Exception as e:
        st.error("An error occurred in the Data Management tab:")
        st.exception(e)


# --- Tab 2: Edit Inventory Record ---
with tab2:
    try:
        st.header("✏️ Edit Inventory Record for Scenario Testing")
        st.info(f"Select a Product/Store pair to modify key fields in `{INVENTORY_CSV}`.")

        # Initialize Session State Variables if they don't exist
        if 'df_inventory_edit' not in st.session_state: st.session_state.df_inventory_edit = None
        if 'record_index_edit' not in st.session_state: st.session_state.record_index_edit = None
        if 'current_pid_edit' not in st.session_state: st.session_state.current_pid_edit = ""
        if 'current_sid_edit' not in st.session_state: st.session_state.current_sid_edit = ""

        # Input Fields for IDs - Use stored values for persistence
        col_edit1, col_edit2 = st.columns([1, 1])
        with col_edit1:
            edit_pid_str_t2 = st.text_input("Enter ProductID:", value=st.session_state.current_pid_edit, key="edit_pid_input_t2")
        with col_edit2:
            edit_sid_str_t2 = st.text_input("Enter StoreID:", value=st.session_state.current_sid_edit, key="edit_sid_input_t2")

        # Load data and find record logic - Trigger on change
        if (edit_pid_str_t2 and edit_sid_str_t2 and \
           (st.session_state.current_pid_edit != edit_pid_str_t2 or st.session_state.current_sid_edit != edit_sid_str_t2)):

            st.session_state.current_pid_edit = edit_pid_str_t2 # Update state
            st.session_state.current_sid_edit = edit_sid_str_t2
            st.session_state.record_index_edit = None # Reset find results
            st.session_state.df_inventory_edit = None

            st.info(f"Searching for P:{edit_pid_str_t2}/S:{edit_sid_str_t2}...")
            loaded_df_t2 = load_csv_data(INVENTORY_CSV)
            if loaded_df_t2 is not None:
                st.session_state.df_inventory_edit = loaded_df_t2 # Store loaded df
                found_index_t2 = find_record_index(loaded_df_t2, edit_pid_str_t2, edit_sid_str_t2)
                if found_index_t2 is not None:
                    st.session_state.record_index_edit = found_index_t2 # Store found index
                    st.success(f"Found record at index {found_index_t2}.")
                    # Don't rerun here, let the rest of the script use the new state
                else:
                    st.warning(f"No record found for P:{edit_pid_str_t2}/S:{edit_sid_str_t2}.")
            else:
                st.error(f"Failed to load `{INVENTORY_CSV}`.")

        st.divider()

        # Editing Section - Display if record index is valid
        if st.session_state.record_index_edit is not None and st.session_state.df_inventory_edit is not None:
            record_index_t2 = st.session_state.record_index_edit
            df_inventory_t2 = st.session_state.df_inventory_edit
            try:
                if record_index_t2 >= len(df_inventory_t2):
                     st.error("Record index out of bounds. Please re-enter IDs.")
                     st.session_state.record_index_edit = None # Reset state
                else:
                    record_data_t2 = df_inventory_t2.iloc[record_index_t2].to_dict()
                    st.subheader(f"✏️ Edit Fields for P:{st.session_state.current_pid_edit}/S:{st.session_state.current_sid_edit}")

                    # Prepare default values safely
                    stock_val = pd.to_numeric(record_data_t2.get('StockLevel', record_data_t2.get('Stock Levels')), errors='coerce')
                    current_stock = int(stock_val) if pd.notna(stock_val) else 0
                    rop_val = pd.to_numeric(record_data_t2.get('ReorderPoint', record_data_t2.get('Reorder Point')), errors='coerce')
                    current_rop = int(rop_val) if pd.notna(rop_val) else 0
                    lead_val = pd.to_numeric(record_data_t2.get('SupplierLeadTimeDays', record_data_t2.get('Supplier Lead Time (days)')), errors='coerce')
                    current_lead_time = int(lead_val) if pd.notna(lead_val) else 0
                    current_expiry_str = record_data_t2.get('ExpiryDate', record_data_t2.get('Expiry Date', ''))
                    current_expiry_date = None
                    if isinstance(current_expiry_str, str) and current_expiry_str:
                         try: current_expiry_date = datetime.datetime.strptime(current_expiry_str, '%Y-%m-%d').date()
                         except ValueError: current_expiry_date = None

                    # Input Widgets
                    new_stock = st.number_input("Stock Level:", value=current_stock, step=1, key=f"edit_stock_{record_index_t2}")
                    new_rop = st.number_input("Reorder Point:", value=current_rop, step=1, key=f"edit_rop_{record_index_t2}")
                    new_expiry = st.date_input("Expiry Date (YYYY-MM-DD):", value=current_expiry_date, format="YYYY-MM-DD", key=f"edit_expiry_{record_index_t2}")
                    new_lead_time = st.number_input("Supplier Lead Time (days):", value=current_lead_time, step=1, min_value=0, key=f"edit_lead_{record_index_t2}")

                    # Save Button Logic
                    if st.button("💾 Save Inventory Changes", key=f"save_btn_{record_index_t2}", type="primary"):
                        df_to_save = st.session_state.df_inventory_edit
                        if df_to_save is not None and st.session_state.record_index_edit is not None:
                            try:
                                # Determine actual column names in the dataframe just before saving
                                stock_col_save = 'StockLevel' if 'StockLevel' in df_to_save.columns else 'Stock Levels'
                                rop_col_save = 'ReorderPoint' if 'ReorderPoint' in df_to_save.columns else 'Reorder Point'
                                expiry_col_save = 'ExpiryDate' if 'ExpiryDate' in df_to_save.columns else 'Expiry Date'
                                lead_col_save = 'SupplierLeadTimeDays' if 'SupplierLeadTimeDays' in df_to_save.columns else 'Supplier Lead Time (days)'

                                # Update DataFrame using .loc and determined column names
                                df_to_save.loc[st.session_state.record_index_edit, stock_col_save] = new_stock
                                df_to_save.loc[st.session_state.record_index_edit, rop_col_save] = new_rop
                                expiry_str_to_save = new_expiry.strftime('%Y-%m-%d') if new_expiry else ''
                                df_to_save.loc[st.session_state.record_index_edit, expiry_col_save] = expiry_str_to_save
                                df_to_save.loc[st.session_state.record_index_edit, lead_col_save] = new_lead_time

                                # Save
                                if save_csv_data(df_to_save, INVENTORY_CSV):
                                    st.success(f"Changes saved to `{INVENTORY_CSV}`!")
                                    load_csv_data.clear(); st.info("Data saved. Check 'Data Management' or re-run workflow.")
                                    st.session_state.record_index_edit = None; st.session_state.df_inventory_edit = None
                                    st.session_state.current_pid_edit = ""; st.session_state.current_sid_edit = ""
                                    st.rerun()
                                else: st.error("Failed to save changes to CSV file.")
                            except Exception as e: st.error(f"Error applying changes to DataFrame:"); st.exception(e)
                        else: st.error("Cannot save. DataFrame/Index state lost.")

            except Exception as e: st.error(f"Error preparing edit widgets:"); st.exception(e); st.session_state.record_index_edit = None

        elif edit_pid_str_t2 and edit_sid_str_t2: st.info("Enter valid IDs. If record not found, check CSV or use Data Management tab.")
        else: st.info("Enter ProductID and StoreID above to load a record.")

    except Exception as e: st.error("An error occurred in the Edit Inventory Record tab:"); st.exception(e)


# --- Tab 3: Run Workflow ---
with tab3:
    try:
        st.header("Execute MAROS Agent Workflow")
        st.markdown(f"""
        Click below to run the main agent workflow (`{os.path.basename(MAIN_SCRIPT)}`). Agents will analyze data from `{DATA_DIR}/` and generate recommendations.
        """)
        st.warning("Ensure Ollama is running locally for full functionality.")
        st.info("Execution time varies (LLM calls can be slow). Output appears below.")
        if st.button("▶️ Run MAROS Workflow", type="primary", use_container_width=True, key="run_workflow_t3"):
            log_container_t3 = st.container(border=True)
            log_placeholder_t3 = log_container_t3.empty()
            log_output_t3 = "Initializing workflow...\n"
            log_placeholder_t3.code(log_output_t3, language="bash")
            with st.spinner("🤖 MAROS agents collaborating..."):
                for line in run_script(MAIN_SCRIPT):
                     log_output_t3 += line + "\n"
                     log_placeholder_t3.code(log_output_t3, language="bash")
            # --- Final Status Display ---
            if "--- ✅ Script finished successfully ---" in log_output_t3: st.success("✅ Workflow finished successfully!")
            elif "ERROR" in log_output_t3: st.error("❌ Workflow finished with errors. See log above.")
            else: st.warning("🤔 Workflow finished, status unclear. Review log.")
    except Exception as e:
        st.error("An error occurred in the Run Workflow tab:")
        st.exception(e)


# --- Tab 4: View Reports ---
with tab4:
    try:
        st.header("Download Workflow Summary Report")
        st.markdown("Access the latest summary report (`.docx`) generated by the MAROS workflow.")
        report_file_t4 = find_latest_report()
        if report_file_t4:
             st.success(f"📄 Latest report found: `{os.path.basename(report_file_t4)}`")
             try:
                 report_timestamp_t4 = datetime.datetime.fromtimestamp(os.path.getmtime(report_file_t4))
                 st.info(f"🕒 Generated on: {report_timestamp_t4.strftime('%Y-%m-%d %H:%M:%S')}")
                 with open(report_file_t4, "rb") as fp:
                     st.download_button(label="⬇️ Download Report (.docx)", data=fp, file_name=os.path.basename(report_file_t4), mime="application/vnd.openxmlformats-officedocument.wordprocessingml.document", use_container_width=True, key="download_report_t4")
             except Exception as e: st.error(f"Error reading report file for download: {e}")
        else:
            st.warning("No workflow report file (.docx) found in the application directory.")
            st.info("Run the MAROS workflow on the previous tab to generate a report.")
            if st.button("🔄 Check for Reports Again", key="refresh_reports_t4"): st.rerun()
    except Exception as e:
        st.error("An error occurred in the View Reports tab:")
        st.exception(e)