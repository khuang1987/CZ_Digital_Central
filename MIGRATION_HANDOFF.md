# Migration Handoff & Context Summary

**Last Updated:** 2026-02-03
**Previous Location:** OneDrive - Medtronic PLC/Huangkai Files/C_code/CZ_Digital_Central
**New Location:** C:\Apps\CZ_Digital_Central

## 🚀 Project Status
We have just completed a major migration to move the project out of OneDrive to improve performance and stability.

## ✅ Recent Accomplishments
1.  **Dashboard Isolation**: Previously attempted to use Symlinks, now fully migrated to local storage.
2.  **Server Health Dashboard**:
    - **Backend**: API active at `/api/server/stats`.
    - **Frontend**: Page active at `/server`.
    - **Features**: Displays Table Count, Row Count, DB Size, and Top 20 Tables.
    - **Navigation**: Added to Sidebar.
3.  **Legacy Dashboard**: `start_services.bat` was updated to allow running the old Streamlit app (`dashboard/app.py`) for reference.

## 🚧 Active Tasks (Immediate Next Steps)
- **Verification**: The Server Dashboard was just built. You need to verify it works in this new environment.
- **Git Setup**: Since this folder is no longer in OneDrive, ensure Git is initialized and commits are made frequently.

## 🔧 Environment Info
- **Node.js**: Dependencies should be installed via `npm install`.
- **Python**: A new `.venv` needs to be created or re-linked if not present. Use `python -m venv .venv` then `pip install -r requirements.txt`.
- **Database**: Connection strings in `.env` should still work (pointing to localhost or remote SQL).

## 💡 Instructions for the Agent
If you are reading this as a "new" agent instance:
1.  Read `task.md` to get the full checklist.
2.  Assume the "Server Health" task is functionally verified but needs a final check in this new folder.
3.  Continue with any pending UI refinements or new feature requests.
