# Operation Name Cleaning Rules (工序名称清洗规则)

**Last Updated:** 2026-02-01
**Related Scripts:** `data_pipelines/sources/dimension/etl/etl_operation_mapping.py`
**Source Data:** `dbo.dim_operation_mapping`

---

## 1. Objective (目标)
The goal of operation name cleaning is to standardize variations of operation names into a single, unified `display_name` for reporting purposes. This ensures that data from different sources or with different suffixes (e.g., "Outsourced") is aggregated correctly in the dashboard.

**Example:**
*   `Milling`
*   `Milling (Outsourced)`
*   `Milling (OEM)`
*   `Milling (可外协)`
    *   **Target:** All merged into -> **`Milling`**

---

## 2. Methodology: Hybrid Strategy (混合清洗策略)
We employ a two-step "Hybrid" strategy to ensure both precision (manual control) and efficiency (automated standardization).

### Step 1: Dictionary Lookup (Primary)
*   **Source:** `operation_cleaning_rules.csv` (Excel derived)
*   **Logic:** The system first looks up the raw `operation_name` in this dictionary.
*   **Purpose:** Allows users to manually define specific mappings for edge cases or business logic changes.
*   **Action:** If a match is found, `display_name` becomes the mapped value. If not, it defaults to the original `operation_name`.

### Step 2: Universal Regex Polish (Secondary)
*   **Logic:** A Regular Expression (Regex) is applied to the **result** of Step 1.
*   **Pattern:** `[\(（](?:外协|OEM|可外协)[^\)）]*[\)）]`
*   **Purpose:** To automatically strip standard "Outsourced" suffixes that might persist or appear in new data, ensuring cleaner names without requiring manual dictionary updates for every single variant.
*   **Target Suffixes:**
    *   `(外协)` / `（外协）`
    *   `(OEM)`
    *   `(可外协)`
    *   Any content within brackets starting with these keywords.

---

## 3. Data Safety & Architecture (数据安全与架构)

### No Data Loss (无损原则)
*   **Raw Data (`raw_sap_labor_hours`)**: This table is **NEVER modified**. It retains the original operation names exactly as they come from SAP.
*   **Mapping Table (`dim_operation_mapping`)**: All cleaning happens in this dimension table.
    *   `operation_name`: Original raw name (Link key).
    *   `display_name`: Cleaned name (used for UI display and grouping).

### Dashboard Usage
*   The PowerBI Dashboard and Web API join `raw_sap_labor_hours` with `dim_operation_mapping` on `operation_name`.
*   Filters and Charts use `dim_operation_mapping.display_name` to present the unified view.

---

## 4. Maintenance (维护指南)
To update the cleaning rules:

1.  **Run ETL Script:**
    The cleaning logic is embedded in the ETL process. To apply changes (e.g., after updating the CSV or code), run:
    ```bash
    python data_pipelines/sources/dimension/etl/etl_operation_mapping.py
    ```
    *(Note: The script automatically handles table updates.)*

2.  **Verify Results:**
    Check the `dim_operation_mapping` table in SQL Server to ensure `display_name` is updated as expected.
