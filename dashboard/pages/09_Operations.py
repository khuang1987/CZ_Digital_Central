import streamlit as st
import subprocess
import os
import sys
import time
import uuid
from pathlib import Path

# 设置页面 (Wide mode essential for this layout)
st.set_page_config(page_title="Pipeline Console", page_icon="⚡", layout="wide")

# 获取项目根目录
PROJECT_ROOT = Path(__file__).resolve().parents[2]

# --- State Management ---
if 'log_buffer' not in st.session_state:
    st.session_state.log_buffer = "🚀 Ready to start...\n"
    
if 'task_status' not in st.session_state:
    st.session_state.task_status = {} # dynamic keys
    
if 'running_task' not in st.session_state:
    st.session_state.running_task = None 

# --- Helper Functions ---

def append_log(text):
    st.session_state.log_buffer += text
    
def clear_log():
    st.session_state.log_buffer = ""

def get_status_color(status):
    if status == "running": return "🟡"
    if status == "success": return "🟢"
    if status == "error": return "🔴"
    return "⚪"

def run_task_logic(task_key, command_list, log_placeholder, cwd=None):
    if cwd is None:
        cwd = str(PROJECT_ROOT)
        
    st.session_state.running_task = task_key
    st.session_state.task_status[task_key] = "running"
    
    env = os.environ.copy()
    env["PYTHONIOENCODING"] = "utf-8"
    env["PYTHONUTF8"] = "1"
    
    clear_log()
    cmd_str = " ".join(command_list)
    append_log(f"--- 启动任务: {task_key} ---\nCMD: {cmd_str}\n\n")
    log_placeholder.text_area("Console Output", value=st.session_state.log_buffer, height=300, disabled=True)
    
    process = subprocess.Popen(
        command_list,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        cwd=cwd,
        env=env,
        text=False, 
        bufsize=1
    )
    
    for line in iter(process.stdout.readline, b''):
        if line:
            try:
                decoded_line = line.decode('utf-8')
            except UnicodeDecodeError:
                try:
                    decoded_line = line.decode('gbk', errors='replace')
                except:
                    decoded_line = line.decode('utf-8', errors='replace')
            append_log(decoded_line)
            # Throttle updates slightly to avoid UI freeze on fast logs? No, direct is usually fine for local.
            log_placeholder.text_area("Console Output", value=st.session_state.log_buffer, height=300, disabled=True, key=f"log_{uuid.uuid4()}")
            
    process.stdout.close()
    return_code = process.wait()
    
    st.session_state.running_task = None
    if return_code == 0:
        st.session_state.task_status[task_key] = "success"
        append_log(f"\n✅ SUCCESS\n")
    else:
        st.session_state.task_status[task_key] = "error"
        append_log(f"\n❌ FAILED (Code: {return_code})\n")
    
    log_placeholder.text_area("Console Output", value=st.session_state.log_buffer, height=300, disabled=True, key=f"log_final_{uuid.uuid4()}")


# --- CSS Styling ---
st.markdown("""
<style>
/* Custom Card Styles */
.main_card {
    background-color: #0E1117;
    border: 1px solid #262730;
    border-radius: 10px;
    padding: 20px;
}
.sub_btn {
    border: 1px solid #444; 
    border-radius: 5px;
    text-align: center;
    font-size: 0.8em;
}
div.stButton > button {
    width: 100%;
}
h3 { margin-top: 0 !important; }
</style>
""", unsafe_allow_html=True)

# --- Layout ---

st.subheader("Pipeline Flow Console")
st.write("") # Spacer

# Master Layout: Left (Master) vs Right (Stages)
col_master, col_spacer, col_stages = st.columns([1, 0.1, 2.8])

is_locked = st.session_state.running_task is not None

# === LEFT: MASTER CONTROLLER ===
with col_master:
    with st.container(border=True):
        st.markdown("### ⚡ Master Controller")
        st.markdown("**Full Auto Mode**")
        st.caption("Executes all pipeline stages sequentially with error recovery.")
        st.write("")
        st.write("")
        
        # Big Play Button
        key = "full_auto"
        status = st.session_state.task_status.get(key, "idle")
        if status == "running":
            st.info("Running...")
        else:
            if st.button("▶ RUN PIPELINE", type="primary", disabled=is_locked, use_container_width=True):
                st.session_state.trigger_task = (key, [sys.executable, "scripts/orchestration/run_etl_parallel.py"])
        
        st.write("")
        st.caption(f"Status: {status.upper()}")

# === RIGHT: STAGES ===
with col_stages:
    
    # --- STAGE 1: INGESTION ---
    with st.container(border=True):
        c_main, c_subs = st.columns([1, 2])
        
        # Main Node
        with c_main:
            st.markdown("#### ☁️ Data Ingestion")
            status = st.session_state.task_status.get("collection_all", "idle")
            st.caption(f"Status: {status.upper()}")
            # Main Run Button (Icon style)
            if st.button("▶ Run Stage", key="btn_ingest", disabled=is_locked):
                st.session_state.trigger_task = ("collection_all", [sys.executable, "scripts/orchestration/run_data_collection.py", "all"])
            if status == "running": st.progress(50) # Fake progress visual
            
        # Sub Tasks (Chips)
        with c_subs:
            st.markdown("**Sub-Tasks**")
            sc1, sc2, sc3 = st.columns(3)
            with sc1:
                if st.button("Planner", key="sub_planner", disabled=is_locked):
                    st.session_state.trigger_task = ("col_planner", [sys.executable, "scripts/orchestration/run_data_collection.py", "planner", "--no-headless"])
            with sc2:
                if st.button("CMES/MES", key="sub_cmes", disabled=is_locked):
                    st.session_state.trigger_task = ("col_cmes", [sys.executable, "scripts/orchestration/run_data_collection.py", "cmes", "--no-headless"])
            with sc3:
                if st.button("Labor", key="sub_labor", disabled=is_locked):
                    st.session_state.trigger_task = ("col_labor", [sys.executable, "scripts/orchestration/run_data_collection.py", "labor"])

    # --- STAGE 2: CLEANING ---
    with st.container(border=True):
        c_main, c_subs = st.columns([1, 2])
        
        with c_main:
            st.markdown("#### 🧹 Data Cleaning")
            status = st.session_state.task_status.get("etl_all", "idle")
            st.caption(f"Status: {status.upper()}")
            if st.button("▶ Run Stage", key="btn_clean", disabled=is_locked):
                # Note: Currently points to full ETL script, might need specific stage filter if implemented
                st.session_state.trigger_task = ("etl_all", [sys.executable, "scripts/orchestration/run_etl_parallel.py"])
        
        with c_subs:
            st.markdown("**Sub-Tasks**")
            sc1, sc2, sc3 = st.columns(3)
            with sc1:
                if st.button("SAP Raw", key="sub_sap", disabled=is_locked):
                    st.session_state.trigger_task = ("etl_sap", [sys.executable, "data_pipelines/sources/sap/etl/etl_sap_routing_raw.py"])
            with sc2:
                if st.button("SFC Batch", key="sub_sfc", disabled=is_locked):
                    st.session_state.trigger_task = ("etl_sfc", [sys.executable, "data_pipelines/sources/sfc/etl/etl_sfc_batch_output_raw.py"])
            with sc3:
                if st.button("MES Batch", key="sub_mes", disabled=is_locked):
                    st.session_state.trigger_task = ("etl_mes", [sys.executable, "data_pipelines/sources/mes/etl/etl_mes_batch_output_raw.py"])

    # --- STAGE 3: OUTPUT ---
    with st.container(border=True):
        c_main, c_subs = st.columns([1, 2])
        
        with c_main:
            st.markdown("#### 📦 Output Generation")
            status = st.session_state.task_status.get("output", "idle")
            st.caption(f"Status: {status.upper()}")
            if st.button("▶ Run Stage", key="btn_out", disabled=is_locked):
                 st.session_state.trigger_task = ("output", [sys.executable, "scripts/orchestration/export_core_to_a1.py", "--mode", "partitioned"])
        
        with c_subs:
             st.markdown("**Sub-Tasks**")
             sc1, sc2 = st.columns(2)
             with sc1:
                 st.caption("Partitioned Parquet")
             with sc2:
                 st.caption("Validation Check")


# --- CONSOLE ---
st.write("")
st.subheader("EXECUTION CONSOLE")
log_placeholder = st.empty()
log_placeholder.text_area("Console", value=st.session_state.log_buffer, height=250, disabled=True, label_visibility="collapsed")


# --- Logic Trigger ---
if 'trigger_task' in st.session_state:
    task_name, cmd = st.session_state.trigger_task
    del st.session_state.trigger_task
    run_task_logic(task_name, cmd, log_placeholder)
    st.rerun()
