"""
Task tracker audit pipeline.

This module pulls active project trackers from ClickUp, validates and reads
eligible Google Sheets tracker tabs, generates per-tracker helper sheets
(`Project Data Collection`, `Dashboard`, and `Datewise Summary`), and writes
pipeline audit outputs into a central audit spreadsheet.

High-level flow:
1. Load runtime configuration from environment variables or local fallback
   files.
2. Authenticate to Google Sheets using a service account.
3. Fetch matching ClickUp projects from the configured list.
4. For each project tracker:
   - open the tracker sheet
   - validate QAI tab header structure
   - generate tracker-level dashboard sheets
   - collect valid rows for audit aggregation
   - record tab- and project-level execution logs
5. Write pipeline execution logs and tracker format issues back to the audit
   workbook.

The script is designed to be safe for unattended execution in GitHub Actions.
It includes retry logic for Google Sheets quota pressure, tracker format
auditing, and project-level status reporting so failures are visible in the
destination spreadsheet instead of being silently ignored.
"""

import os
import re
import time
import sys
import json
import base64
import requests
import pandas as pd
from datetime import datetime, timezone
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from tempfile import NamedTemporaryFile
from dotenv import load_dotenv
from tqdm import tqdm
from gspread.utils import rowcol_to_a1
from collections import defaultdict

# ============================================================
# LOGGER
# ============================================================

def log(msg):
    """Print a UTC timestamped log line for local runs and GitHub Actions."""
    try:
        # Sanitize message to handle invalid Unicode, including lone surrogates.
        if isinstance(msg, str):
            msg = msg.encode("utf-8", errors="backslashreplace").decode("utf-8", errors="replace")
        timestamp = datetime.now(timezone.utc).isoformat()
        print(f"[{timestamp}] {msg}", flush=True)
    except Exception as e:
        # Fallback if even this fails
        print(f"[TIMESTAMP] ERROR: Could not log message: {e}", flush=True)

READ_DELAY = 2.0  # Prevent quota bursts (increased)
WRITE_DELAY = 2.0  # Delay between write operations
MAX_RETRIES = 15  # Maximum retry attempts for quota errors
MAX_QUOTA_WAIT = 300  # Maximum total wait time (5 minutes)

def sanitize_utf8(value):
    """Sanitize a value to ensure valid UTF-8 encoding."""
    if value is None:
        return ""
    try:
        text = str(value)
        return text.encode("utf-8", errors="backslashreplace").decode("utf-8", errors="replace")
    except Exception:
        return str(value)[:100]  # Return first 100 chars as fallback

# ============================================================
# ENV VARIABLES
# ============================================================

load_dotenv()

CLICKUP_API_TOKEN = None
GOOGLE_SERVICE_ACCOUNT_JSON = None
AUDIT_SHEET_URL = None
PROJECT_NAME_FILTER = None
PROJECT_NAME_FILTERS = []
IGNORED_DELIVERY_LEAD_EMAILS = set()
HIGHLIGHT_TRACKER_ERRORS = None
RESOURCE_LOOKUP_TAB = "Team List & Activity"
gs_client = None
audit_sheet = None
resource_type_map = {}

def normalize_service_account_json(value):
    """Normalize a service account secret from raw JSON, file path, or base64."""
    if not value:
        return value

    text = str(value).strip()

    # Allow the env var to point to a local JSON file path.
    if os.path.exists(text) and os.path.isfile(text):
        with open(text, encoding="utf-8") as service_account_file:
            text = service_account_file.read().strip()

    # Handle secrets that escape newlines.
    if "\\n" in text and "{\n" not in text:
        text = text.replace("\\n", "\n")

    try:
        json.loads(text)
        return text
    except Exception:
        pass

    # Handle base64-encoded JSON.
    try:
        decoded = base64.b64decode(text).decode("utf-8")
        json.loads(decoded)
        return decoded
    except Exception:
        return text

def parse_env_list(value):
    """Parse a comma- or newline-separated env var into trimmed non-empty values."""
    if not value:
        return []

    return [
        item.strip()
        for item in re.split(r"[\n,]+", str(value))
        if item and item.strip()
    ]

def load_runtime_config():
    """Load required runtime configuration and stop early if anything is missing."""
    global CLICKUP_API_TOKEN
    global GOOGLE_SERVICE_ACCOUNT_JSON
    global AUDIT_SHEET_URL
    global PROJECT_NAME_FILTER
    global PROJECT_NAME_FILTERS
    global IGNORED_DELIVERY_LEAD_EMAILS
    global HIGHLIGHT_TRACKER_ERRORS
    global RESOURCE_LOOKUP_TAB

    CLICKUP_API_TOKEN = os.getenv("CLICKUP_API_TOKEN")
    GOOGLE_SERVICE_ACCOUNT_JSON = os.getenv("GOOGLE_SERVICE_ACCOUNT_JSON")
    AUDIT_SHEET_URL = os.getenv("AUDIT_SHEET_URL")
    PROJECT_NAME_FILTER = (os.getenv("PROJECT_NAME_FILTER") or "").strip()
    PROJECT_NAME_FILTERS = parse_env_list(PROJECT_NAME_FILTER)
    IGNORED_DELIVERY_LEAD_EMAILS = {
        email.lower()
        for email in parse_env_list(os.getenv("IGNORE_PDL_EMAILS"))
    }
    HIGHLIGHT_TRACKER_ERRORS = (os.getenv("HIGHLIGHT_TRACKER_ERRORS", "true") or "true").strip().lower() in {
        "1", "true", "yes", "y"
    }
    RESOURCE_LOOKUP_TAB = os.getenv("RESOURCE_LOOKUP_TAB", "Team List & Activity")

    if not GOOGLE_SERVICE_ACCOUNT_JSON and os.path.exists("service_account.json"):
        with open("service_account.json", encoding="utf-8") as service_account_file:
            GOOGLE_SERVICE_ACCOUNT_JSON = service_account_file.read()

    GOOGLE_SERVICE_ACCOUNT_JSON = normalize_service_account_json(GOOGLE_SERVICE_ACCOUNT_JSON)

    required_env = {
        "CLICKUP_API_TOKEN": CLICKUP_API_TOKEN,
        "GOOGLE_SERVICE_ACCOUNT_JSON or service_account.json": GOOGLE_SERVICE_ACCOUNT_JSON,
        "AUDIT_SHEET_URL": AUDIT_SHEET_URL,
    }

    missing_env = [k for k, v in required_env.items() if not v]
    if missing_env:
        log(f"❌ Missing required environment variables: {', '.join(missing_env)}")
        sys.exit(1)

def authenticate_google():
    """Authenticate gspread and open the destination audit spreadsheet."""
    global gs_client
    global audit_sheet

    sa_path = None
    try:
        with NamedTemporaryFile(mode="w+", suffix=".json", delete=False) as f:
            f.write(GOOGLE_SERVICE_ACCOUNT_JSON)
            sa_path = f.name

        scope = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive",
        ]

        creds = ServiceAccountCredentials.from_json_keyfile_name(sa_path, scope)
        gs_client = gspread.authorize(creds)

        audit_sheet = gs_client.open_by_url(AUDIT_SHEET_URL)

        log("✅ Google authenticated")
    except Exception as e:
        log(f"❌ Google authentication failed: {e}")
        sys.exit(1)
    finally:
        # Clean up temporary credentials file
        if sa_path and os.path.exists(sa_path):
            try:
                os.remove(sa_path)
            except Exception as cleanup_error:
                log(f"⚠️ Failed to clean up temp credentials file: {cleanup_error}")

# ============================================================
# CLICKUP FETCH
# ============================================================

LIST_ID = "900201326056"
TRACKER_FIELD_NAME = "Task/Progress Tracker"
DATA_QUANTITY_FIELD_NAME = "Data Quantity"
DELIVERY_LEAD_EMAIL_FIELD_NAMES = {"Delivery Lead Email", "Delivery lead"}
PDL_EMAIL_FIELD_NAMES = {"PDL Email"}
PDL_EMAIL_FIELD_IDS = {"2b95d848-3486-4e91-9d41-55de07011be4"}

TARGET_STATUS_NAMES = [
    "project in progress",
    "project received",
    "pending schedule approval"
]

def fetch_clickup_tasks():
    """Fetch all matching ClickUp tasks from the configured list with paging."""
    headers = {"Authorization": CLICKUP_API_TOKEN}
    tasks = []
    page = 0

    while True:
        r = requests.get(
            f"https://api.clickup.com/api/v2/list/{LIST_ID}/task",
            headers=headers,
            params={
                "page": page,
                "include_closed": True,
                "include_archived": True,
                "statuses[]": TARGET_STATUS_NAMES,
            },
            timeout=30,
        )

        if r.status_code != 200:
            log(f"❌ ClickUp API error: HTTP {r.status_code}")
            sys.exit(1)

        batch = r.json().get("tasks", [])
        if not batch:
            break

        tasks.extend(batch)
        page += 1

    log(f"✅ Total ClickUp projects (selected status): {len(tasks)}")
    return tasks

def get_custom_field_value(task, field_names=None, field_ids=None, default=""):
    """Return the first matching ClickUp custom field value by name or id."""
    field_names = field_names or set()
    field_ids = field_ids or set()

    if isinstance(field_names, str):
        field_names = {field_names}
    if isinstance(field_ids, str):
        field_ids = {field_ids}

    normalized_field_names = {str(name).strip().lower() for name in field_names}
    normalized_field_ids = {str(field_id).strip().lower() for field_id in field_ids}

    for field in task.get("custom_fields", []):
        field_name = str(field.get("name", "")).strip().lower()
        field_id = str(field.get("id", "")).strip().lower()
        if field_name in normalized_field_names or field_id in normalized_field_ids:
            return field.get("value") or default
    return default

def extract_email_values(value):
    """Flatten a ClickUp custom field value into normalized email strings."""
    if value is None:
        return set()

    if isinstance(value, str):
        return {item.strip().lower() for item in parse_env_list(value)}

    if isinstance(value, (list, tuple, set)):
        emails = set()
        for item in value:
            emails.update(extract_email_values(item))
        return emails

    if isinstance(value, dict):
        emails = set()
        for key in ("email", "value", "username", "name"):
            if key in value:
                emails.update(extract_email_values(value.get(key)))
        return emails

    return {str(value).strip().lower()} if str(value).strip() else set()

def get_task_pdl_email_values(task):
    """Return normalized email values from the task PDL custom field."""
    return extract_email_values(
        get_custom_field_value(
            task,
            field_names=PDL_EMAIL_FIELD_NAMES,
            field_ids=PDL_EMAIL_FIELD_IDS,
            default="",
        )
    )

def get_task_delivery_lead_email(task):
    """Return the raw Delivery Lead Email field value for audit/export use."""
    value = get_custom_field_value(
        task,
        field_names=DELIVERY_LEAD_EMAIL_FIELD_NAMES,
        default="",
    )
    if isinstance(value, str):
        return value.strip()

    email_values = sorted(extract_email_values(value))
    return ", ".join(email_values)

def filter_tasks_for_test_run(tasks):
    """Optionally skip tasks by project name and ignored PDL email."""
    filtered_tasks = tasks

    if PROJECT_NAME_FILTERS:
        ignored_project_names = {name.lower() for name in PROJECT_NAME_FILTERS}
        filtered_tasks = [
            task for task in filtered_tasks
            if str(task.get("name", "")).strip().lower() not in ignored_project_names
        ]
        log(
            f"🔕 PROJECT_NAME_FILTER enabled for {len(PROJECT_NAME_FILTERS)} project name(s) "
            f"-> remaining {len(filtered_tasks)} project(s) after ignore list"
        )
    else:
        log("ℹ️ PROJECT_NAME_FILTER is empty. Processing all matching ClickUp projects.")

    if not IGNORED_DELIVERY_LEAD_EMAILS:
        return filtered_tasks

    tasks_after_pdl_filter = []
    ignored_task_count = 0
    tasks_with_pdl_value = 0

    for task in filtered_tasks:
        delivery_lead_email_values = get_task_pdl_email_values(task)
        if delivery_lead_email_values:
            tasks_with_pdl_value += 1

        if delivery_lead_email_values.intersection(IGNORED_DELIVERY_LEAD_EMAILS):
            ignored_task_count += 1
            continue

        tasks_after_pdl_filter.append(task)

    log(
        f"🔕 IGNORE_PDL_EMAILS enabled for {len(IGNORED_DELIVERY_LEAD_EMAILS)} email(s) "
        f"-> skipped {ignored_task_count} project(s)"
    )
    log(
        f"ℹ️ PDL email values found on {tasks_with_pdl_value}/{len(filtered_tasks)} project(s) "
        f"after project-name filtering"
    )
    return tasks_after_pdl_filter

# ============================================================
# EXTRACTION CONFIG
# ============================================================

# Columns used for extraction/aggregation. These are present in both valid tracker formats.
REQUIRED_COLUMNS = [
    "Annotation Task Name",
    "Annotation Date",
    "QAI ID And Annotator Name",
    "QC Date",
    "QAI ID And Reviewer Name",
    "QC Verdict",
    "CC Date",
    "QAI ID And Cross Checker Name",
    "CC Verdict",
]

# Full A:T tracker formats allowed for every working tracker tab.
# Format 1 = without Frame Count. Format 2 = with Frame Count in column D.
TRACKER_FORMAT_WITHOUT_FRAME_COUNT = [
    "Annotation Task Name",
    "Annotation Date",
    "QAI ID And Annotator Name",
    "Annotation Confused",
    "Annotator Remarks",
    "Extra Column",
    "Extra Column",
    "QC Date",
    "QAI ID And Reviewer Name",
    "QC Verdict",
    "Error Count(Object+Tag)",
    "Total Objects",
    "QC Remarks",
    "Extra Column",
    "CC Date",
    "QAI ID And Cross Checker Name",
    "CC Verdict",
    "Error Count(Object+Tag)",
    "CC Remarks",
    "Extra Column",
]

TRACKER_FORMAT_WITH_FRAME_COUNT = [
    "Annotation Task Name",
    "Annotation Date",
    "QAI ID And Annotator Name",
    "Frame Count",
    "Annotation Confused",
    "Annotator Remarks",
    "Extra Column",
    "QC Date",
    "QAI ID And Reviewer Name",
    "QC Verdict",
    "Error Count(Object+Tag)",
    "Total Objects",
    "QC Remarks",
    "Extra Column",
    "CC Date",
    "QAI ID And Cross Checker Name",
    "CC Verdict",
    "Error Count(Object+Tag)",
    "CC Remarks",
    "Extra Column",
]

ALLOWED_TRACKER_FORMATS = {
    "WITHOUT_FRAME_COUNT": TRACKER_FORMAT_WITHOUT_FRAME_COUNT,
    "WITH_FRAME_COUNT": TRACKER_FORMAT_WITH_FRAME_COUNT,
}

# Normalization only for validation. It protects against accidental double spaces
# such as "Annotator  Remarks".
def normalize_header_cell(value):
    """Normalize a tracker header cell for format comparison only."""
    return re.sub(r"\s+", " ", str(value or "").strip()).lower()

def normalize_header_row(header):
    """Normalize the first 20 header columns used by tracker validation."""
    return [normalize_header_cell(c) for c in header[:20]]

def validate_tracker_header(header):
    """Validate that required columns are present (flexible approach).
    
    Instead of strict A:T format matching, check if all required columns exist
    in the header (case-insensitive, position-independent).
    Returns: (is_valid, format_name, mismatches)
    """
    if not header:
        return False, "EMPTY_HEADER", []
    
    # Normalize header for matching (case-insensitive, extra whitespace removed)
    normalized_header = {normalize_header_cell(h) for h in header if h}
    normalized_required = {normalize_header_cell(col) for col in REQUIRED_COLUMNS}
    
    missing_columns = normalized_required - normalized_header
    
    if not missing_columns:
        # All required columns found - data is valid
        return True, "FLEXIBLE_MATCH", []
    else:
        # Some required columns missing
        mismatches = [
            {
                "column_letter": "?",
                "column_number": 0,
                "expected": col,
                "actual": "",
            }
            for col in sorted(list(missing_columns))[:5]  # Report first 5 missing
        ]
        return False, "MISSING_REQUIRED_COLUMNS", mismatches

tracker_format_issue_logs = []

def log_tracker_format_issue(
    project_name,
    tracker_url,
    tab_name,
    issue_status,
    matched_format="",
    mismatch_count=0,
    mismatch_details="",
    actual_header="",
    delivery_lead_email="",
    data_type="",
):
    """Capture one tracker format audit record for downstream reporting."""
    tracker_format_issue_logs.append({
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "project_name": project_name,
        "tracker_url": tracker_url,
        "tab_name": tab_name,
        "issue_status": issue_status,
        "closest_expected_format": matched_format,
        "mismatch_count": mismatch_count,
        "mismatch_details": mismatch_details,
        "actual_a_to_t_header": actual_header,
        "delivery_lead_email": delivery_lead_email,
        "data_type": data_type,
    })

STAGE_REQUIRED_COLUMNS = {
    "ANNOTATION": ["Annotation Date", "QAI ID And Annotator Name"],
    "QC": ["QC Date", "QAI ID And Reviewer Name"],
    "CC": ["CC Date", "QAI ID And Cross Checker Name"],
}

ROW_COMPLETENESS_RULES = {
    "ANNOTATION_SET": ["Annotation Task Name", "Annotation Date", "QAI ID And Annotator Name"],
    "QC_SET": ["QC Date", "QAI ID And Reviewer Name", "QC Verdict"],
    "CC_SET": ["CC Date", "QAI ID And Cross Checker Name", "CC Verdict"],
}

SKIP_SHEETS = {
    "Dashboard",
    "Project Data Collection",
    "Annotation Progress",
    "Info",
    "Cross Check Progress",
    "QC Errors",
    "CC Errors",
    "client",
    "Datewise Summary",
}

VALID_TAB_QAI_REGEX = re.compile(r"QAI_", re.IGNORECASE)
QAI_ID_REGEX = re.compile(r"(QAI[\s_-]*[A-Z]{2,}\d+)", re.IGNORECASE)
TRACKER_COMBINED_SHEET_NAME = "Project Data Collection"
TRACKER_DASHBOARD_SHEET_NAME = "Dashboard"
TRACKER_DATEWISE_SUMMARY_SHEET_NAME = "Datewise Summary"
NO_DATA_FETCHED_REASON = "PROJECT_NOT_STARTED_OR_TRACKER_IS_EMPTY"

tab_audit_logs = []
project_status_map = {}
dashboard_annotation_rows = []
dashboard_review_rows = []
project_run_logs = []
PROJECT_RUN_LOG_COLUMNS = [
    "run_timestamp",
    "project_name",
    "clickup_task_id",
    "clickup_task_url",
    "clickup_status",
    "tracker_url",
    "tracker_sheet_id",
    "delivery_lead_email",
    "data_type",
    "data_quantity",
    "compiled_into_report",
    "final_status_code",
    "final_reason",
    "total_worksheets",
    "eligible_qai_tabs",
    "tabs_added",
    "tabs_skipped",
    "tabs_errored",
    "dashboard_tabs_seen",
    "dashboard_rows_found",
    "compiled_rows_added",
    "last_successful_tab",
    "last_issue_tab",
    "last_issue_reason_code",
]

PROJECT_STATUS_REASON_MAP = {
    "NO_VALID_DATA": "Project started processing, but no final data status was determined.",
    "NO_TRACKER": "Tracker URL is missing from the ClickUp custom field.",
    "INVALID_TRACKER_URL": "Tracker URL exists, but the Google Sheet ID could not be parsed.",
    "SHEET_OPEN_FAILED": "Google Sheet could not be opened after retry attempts.",
    "READ_QUOTA_FAILED": "A required tracker tab could not be read because the quota/retries were exhausted.",
    "NO_VALID_ROWS": "Eligible tracker tabs were found, but no valid rows were added to the compiled report.",
    "PROCESSED_SUCCESS": "Project data was successfully compiled into the destination report.",
}

def log_tab_activity(
    project_name,
    tracker_url,
    tab_name,
    status,
    reason,
    delivery_lead_email="",
    data_type="",
    total_rows=0,
    rows_added=0,
):
    """Store one per-tab processing event for the central audit log sheet."""
    tab_audit_logs.append(
        {
            "project_name": project_name,
            "tracker_url": tracker_url,
            "tab_name": tab_name,
            "status": status,
            "reason": reason,
            "delivery_lead_email": delivery_lead_email,
            "data_type": data_type,
            "total_rows_in_tab": total_rows,
            "rows_added": rows_added,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
    )

def build_project_log_row(task, tracker_url="", data_type="", delivery_lead_email="", data_quantity=""):
    """Create the default project execution log payload before processing begins."""
    return {
        "run_timestamp": datetime.now(timezone.utc).isoformat(),
        "project_name": task.get("name", ""),
        "clickup_task_id": task.get("id", ""),
        "clickup_task_url": task.get("url", ""),
        "clickup_status": (task.get("status") or {}).get("status", ""),
        "tracker_url": tracker_url or "",
        "tracker_sheet_id": "",
        "delivery_lead_email": delivery_lead_email or "",
        "data_type": data_type or "",
        "data_quantity": data_quantity,
        "compiled_into_report": "NO",
        "final_status_code": "NO_VALID_DATA",
        "final_reason": PROJECT_STATUS_REASON_MAP["NO_VALID_DATA"],
        "total_worksheets": 0,
        "eligible_qai_tabs": 0,
        "tabs_added": 0,
        "tabs_skipped": 0,
        "tabs_errored": 0,
        "dashboard_tabs_seen": 0,
        "dashboard_rows_found": 0,
        "compiled_rows_added": 0,
        "last_successful_tab": "",
        "last_issue_tab": "",
        "last_issue_reason_code": "",
    }

def finalize_project_log(project_log, status_code, fallback_reason=""):
    """Finalize and store a project execution log row."""
    project_log["final_status_code"] = status_code
    project_log["compiled_into_report"] = "YES" if status_code == "PROCESSED_SUCCESS" else "NO"
    project_log["final_reason"] = fallback_reason or PROJECT_STATUS_REASON_MAP.get(
        status_code,
        "Project processing finished with an unmapped status.",
    )
    project_run_logs.append(project_log)

def upload(df, sheet, tab):
    """Replace a destination worksheet with the contents of a DataFrame with retry logic."""
    total_wait = 0
    
    for attempt in range(MAX_RETRIES):
        try:
            time.sleep(WRITE_DELAY)
            try:
                ws = sheet.worksheet(tab)
                ws.clear()
            except Exception:
                try:
                    ws = sheet.add_worksheet(title=tab, rows="1000", cols="20")
                except Exception as e:
                    if "already exists" not in str(e).lower():
                        raise
                    ws = sheet.worksheet(tab)
                    ws.clear()

            # Sanitize DataFrame for UTF-8 encoding
            try:
                clean_df = df.copy()
                # Apply UTF-8 sanitization to all object columns
                for col in clean_df.columns:
                    if clean_df[col].dtype == 'object':
                        clean_df[col] = clean_df[col].apply(sanitize_utf8)
            except Exception as e:
                log(f"⚠️ Error sanitizing DataFrame for {tab}: {e}")
                clean_df = df.copy()

            # Sanitize column names
            safe_columns = [sanitize_utf8(col) for col in clean_df.columns]
            ws.update(range_name="A1", values=[safe_columns])

            if not clean_df.empty:
                time.sleep(WRITE_DELAY)
                clean_df = clean_df.replace([float("inf"), float("-inf")], pd.NA)
                clean_df = clean_df.where(pd.notna(clean_df), "")
                values = clean_df.astype(str).values.tolist()
                # Final sanitization of all cell values
                sanitized_values = [[sanitize_utf8(cell) for cell in row] for row in values]
                ws.update(range_name="A2", values=sanitized_values)
            
            return  # Success
        except Exception as e:
            if is_quota_error(e):
                wait = exponential_backoff(attempt, base=15, max_wait=120)
                total_wait += wait
                
                if total_wait > MAX_QUOTA_WAIT:
                    log(f"❌ Max quota wait time ({MAX_QUOTA_WAIT}s) exceeded. Data may be incomplete.")
                    raise RuntimeError(f"Write quota exceeded for tab: {tab}")
                
                log(f"⏳ Quota hit writing to {tab}, wait {wait}s (attempt {attempt+1}/{MAX_RETRIES})")
                time.sleep(wait)
            else:
                raise
    
    raise RuntimeError(f"Write quota exceeded for tab: {tab}")

def is_quota_error(error):
    """Return True when a Sheets/Drive API error is caused by rate limits."""
    return "429" in str(error) or "quota exceeded" in str(error).lower()

def exponential_backoff(attempt, base=10, max_wait=60):
    """Calculate exponential backoff wait time with cap."""
    wait = base * (2 ** attempt)
    return min(wait, max_wait)

def open_spreadsheet_by_key_with_retry(client, sheet_id, project_name=""):
    """Open a spreadsheet with retry and backoff for quota-related failures."""
    label = project_name or sheet_id
    total_wait = 0

    for attempt in range(MAX_RETRIES):
        try:
            time.sleep(READ_DELAY)
            return client.open_by_key(sheet_id)
        except Exception as e:
            if not is_quota_error(e):
                raise

            wait = exponential_backoff(attempt, base=5, max_wait=120)
            total_wait += wait
            
            if total_wait > MAX_QUOTA_WAIT:
                log(f"❌ Max quota wait time ({MAX_QUOTA_WAIT}s) exceeded for: {label}")
                raise RuntimeError(f"Spreadsheet open quota exceeded: {label}")
            
            log(f"⏳ Quota hit opening spreadsheet: {label}, wait {wait}s (attempt {attempt+1}/{MAX_RETRIES})")
            time.sleep(wait)

    raise RuntimeError(f"Spreadsheet open quota exceeded: {label}")

def list_worksheets_with_retry(spreadsheet, project_name=""):
    """List spreadsheet worksheets with retry and backoff for quota errors."""
    label = project_name or getattr(spreadsheet, "id", "unknown spreadsheet")
    total_wait = 0

    for attempt in range(MAX_RETRIES):
        try:
            time.sleep(READ_DELAY)
            return spreadsheet.worksheets()
        except Exception as e:
            if not is_quota_error(e):
                raise

            wait = exponential_backoff(attempt, base=10, max_wait=120)
            total_wait += wait
            
            if total_wait > MAX_QUOTA_WAIT:
                log(f"❌ Max quota wait time ({MAX_QUOTA_WAIT}s) exceeded for: {label}")
                raise RuntimeError(f"Worksheet metadata quota exceeded: {label}")
            
            log(f"⏳ Quota hit loading worksheet list: {label}, wait {wait}s (attempt {attempt+1}/{MAX_RETRIES})")
            time.sleep(wait)

    raise RuntimeError(f"Worksheet metadata quota exceeded: {label}")

def read_worksheet_values(worksheet, range_name=None, project_name="", sheet_name=""):
    """Read worksheet values with retry and backoff for Sheets API quota errors."""
    label = sheet_name or worksheet.title.strip()
    total_wait = 0

    for attempt in range(MAX_RETRIES):
        try:
            time.sleep(READ_DELAY)
            if range_name:
                return worksheet.get(range_name)
            return worksheet.get_all_values()
        except gspread.exceptions.APIError as e:
            if is_quota_error(e):
                wait = exponential_backoff(attempt, base=10, max_wait=120)
                total_wait += wait
                
                if total_wait > MAX_QUOTA_WAIT:
                    log(f"❌ Max quota wait time ({MAX_QUOTA_WAIT}s) exceeded for: {label}")
                    raise RuntimeError(f"Read quota exceeded for worksheet: {label}")
                
                if project_name:
                    log(f"⏳ Quota hit: {project_name} - {label}, wait {wait}s (attempt {attempt+1}/{MAX_RETRIES})")
                else:
                    log(f"⏳ Quota hit: {label}, wait {wait}s (attempt {attempt+1}/{MAX_RETRIES})")
                time.sleep(wait)
            else:
                raise

    raise RuntimeError(f"Read quota exceeded for worksheet: {label}")

def read_worksheet_ranges(worksheet, range_names, project_name="", sheet_name=""):
    """Read multiple worksheet ranges with retry and backoff for quota errors."""
    label = sheet_name or worksheet.title.strip()
    total_wait = 0

    for attempt in range(MAX_RETRIES):
        try:
            time.sleep(READ_DELAY)
            return worksheet.batch_get(range_names)
        except gspread.exceptions.APIError as e:
            if is_quota_error(e):
                wait = exponential_backoff(attempt, base=10, max_wait=120)
                total_wait += wait

                if total_wait > MAX_QUOTA_WAIT:
                    log(f"❌ Max quota wait time ({MAX_QUOTA_WAIT}s) exceeded for: {label}")
                    raise RuntimeError(f"Read quota exceeded for worksheet: {label}")

                if project_name:
                    log(f"⏳ Quota hit: {project_name} - {label}, wait {wait}s (attempt {attempt+1}/{MAX_RETRIES})")
                else:
                    log(f"⏳ Quota hit: {label}, wait {wait}s (attempt {attempt+1}/{MAX_RETRIES})")
                time.sleep(wait)
            else:
                raise

    raise RuntimeError(f"Read quota exceeded for worksheet: {label}")

def column_number_to_letter(column_number):
    """Convert a 1-based column number to an A1 column letter."""
    letters = []
    while column_number > 0:
        column_number, remainder = divmod(column_number - 1, 26)
        letters.append(chr(65 + remainder))
    return "".join(reversed(letters))

def read_tracker_required_values(worksheet, project_name="", sheet_name=""):
    """Read only the required tracker columns, even if they are not contiguous."""
    header_values = read_worksheet_values(
        worksheet,
        range_name="A:T",
        project_name=project_name,
        sheet_name=sheet_name,
    )
    if not header_values:
        return [], []

    full_header = [str(h).strip() for h in header_values[0][:20]]
    available_col_idx = {
        column_name: full_header.index(column_name)
        for column_name in REQUIRED_COLUMNS
        if column_name in full_header
    }

    column_ranges = []
    range_to_column_name = []
    for column_name in REQUIRED_COLUMNS:
        if column_name not in available_col_idx:
            continue
        column_letter = column_number_to_letter(available_col_idx[column_name] + 1)
        column_ranges.append(f"{column_letter}:{column_letter}")
        range_to_column_name.append(column_name)

    if not column_ranges:
        return [REQUIRED_COLUMNS.copy()], full_header

    column_data = read_worksheet_ranges(
        worksheet,
        column_ranges,
        project_name=project_name,
        sheet_name=sheet_name,
    )

    column_map = {
        column_name: values
        for column_name, values in zip(range_to_column_name, column_data)
    }
    total_rows = max((len(values) for values in column_map.values()), default=1)

    projected_values = [REQUIRED_COLUMNS.copy()]
    for row_index in range(1, total_rows):
        row = []
        for column_name in REQUIRED_COLUMNS:
            values = column_map.get(column_name, [])
            cell = ""
            if row_index < len(values) and values[row_index]:
                cell = values[row_index][0]
            row.append(cell)
        projected_values.append(row)

    return projected_values, full_header

def clear_or_create_worksheet(spreadsheet, title, rows=1000, cols=20):
    """Return a cleared worksheet, creating it first when it does not exist."""
    try:
        ws = spreadsheet.worksheet(title)
        ws.clear()
        return ws
    except Exception:
        pass

    try:
        return spreadsheet.add_worksheet(title=title, rows=str(rows), cols=str(cols))
    except Exception as e:
        if "already exists" not in str(e).lower():
            raise

    ws = spreadsheet.worksheet(title)
    ws.clear()
    return ws

def delete_worksheet_if_exists(spreadsheet, title):
    """Delete a worksheet when present and ignore missing-sheet errors."""
    try:
        ws = spreadsheet.worksheet(title)
        spreadsheet.del_worksheet(ws)
    except Exception:
        pass

def format_header_row(worksheet, start_col, width):
    """Apply a consistent header style to a worksheet column range."""
    start_a1 = rowcol_to_a1(1, start_col)
    end_a1 = rowcol_to_a1(1, start_col + width - 1)
    worksheet.format(
        f"{start_a1}:{end_a1}",
        {
            "textFormat": {"bold": True},
            "backgroundColor": {"red": 0.95, "green": 0.95, "blue": 0.95},
        },
    )

def set_sheet_tab_red(spreadsheet, worksheet):
    """Mark a worksheet tab red to make tracker issues more visible."""
    spreadsheet.batch_update(
        {
            "requests": [
                {
                    "updateSheetProperties": {
                        "properties": {
                            "sheetId": worksheet.id,
                            "tabColor": {"red": 1, "green": 0, "blue": 0},
                        },
                        "fields": "tabColor",
                    }
                }
            ]
        }
    )

def highlight_error_cell(worksheet, cell_a1):
    """Highlight one worksheet cell that corresponds to a detected issue."""
    worksheet.format(
        cell_a1,
        {"backgroundColor": {"red": 1, "green": 0.8, "blue": 0.8}},
    )

def extract_dashboard_member_parts(raw):
    """Split a dashboard member label into QAI identifier and display name."""
    if not raw:
        return "", ""

    text = str(raw).strip()
    match = re.search(r"QAI_[A-Z]+\d+", text, re.IGNORECASE)
    if match:
        member_id = match.group(0).upper()
        member_name = text.replace(match.group(0), "")
        member_name = re.sub(r"[\(\)\-_]+", " ", member_name)
        member_name = re.sub(r"\bQAI\b", " ", member_name, flags=re.IGNORECASE)
        member_name = re.sub(r"\s+", " ", member_name).strip()
        return member_id, member_name

    return text, ""

def remove_duplicate_tracker_rows(rows, header):
    """Remove duplicate tracker rows using task name plus annotator identity."""
    if not rows:
        return rows

    task_name_index = header.index("Annotation Task Name") if "Annotation Task Name" in header else -1
    annotator_index = header.index("QAI ID And Annotator Name") if "QAI ID And Annotator Name" in header else -1

    if task_name_index == -1 or annotator_index == -1:
        return rows

    filtered = [rows[0]]
    seen = set()

    for row in rows[1:]:
        key = f"{row[task_name_index]}___{row[annotator_index]}"
        if key in seen:
            continue
        seen.add(key)
        filtered.append(row)

    return filtered

def extract_valid_tracker_rows_for_merge(values, source_header=None):
    """Extract normalized rows from a tracker tab for the combined sheet."""
    if not values or len(values) < 2:
        return None, "EMPTY_TAB", []

    header = [str(h).strip() for h in (source_header or values[0])[:20]]
    is_valid_format, _, _ = validate_tracker_header(header)
    if not is_valid_format:
        return None, "INVALID_FORMAT", header

    projected_header = [str(h).strip() for h in values[0][:20]]
    available_col_idx = {c: projected_header.index(c) for c in REQUIRED_COLUMNS if c in projected_header}
    supported_stages = [
        stage_name
        for stage_name, stage_cols in STAGE_REQUIRED_COLUMNS.items()
        if all(col in available_col_idx for col in stage_cols)
    ]

    if not supported_stages:
        return None, "NO_SUPPORTED_STAGE_COLUMNS", header

    rows = []
    for r in values[1:]:
        if not any(str(x).strip() for x in r):
            continue

        row_values = [
            r[available_col_idx[c]] if c in available_col_idx and available_col_idx[c] < len(r) else ""
            for c in REQUIRED_COLUMNS
        ]

        has_stage_data = any(
            any(
                str(row_values[REQUIRED_COLUMNS.index(col)]).strip()
                for col in stage_cols
                if row_values[REQUIRED_COLUMNS.index(col)] is not None
            )
            for stage_cols in STAGE_REQUIRED_COLUMNS.values()
        )
        if not has_stage_data:
            continue

        rows.append(row_values)

    if not rows:
        return None, "NO_VALID_ROWS", header

    return {
        "header": header,
        "rows": rows,
    }, "OK", header

def append_sheet_error_once(error_logs, sheet_name, message, header=None, col_name=None, row_number=None):
    """
    Keep one entry per (sheet, message). Increment `count` on repeats.
    Preserve `cell` as the first occurrence for highlighting.
    """
    for e in error_logs:
        if e.get("sheet") == sheet_name and e.get("message") == message:
            e["count"] = e.get("count", 1) + 1
            return

    cell = f"A{row_number or 1}"
    if header and col_name:
        try:
            idx = header.index(col_name)
            cell = f"{chr(65 + idx)}{row_number or 1}"
        except Exception:
            pass

    error_logs.append({"sheet": sheet_name, "cell": cell, "message": message, "count": 1})

def collect_row_completeness_issues(values):
    """Find rows where a required column set is only partially filled."""
    if not values or len(values) < 2:
        return []

    header = [str(h).strip() for h in values[0]]
    col_idx = {column_name: header.index(column_name) for column_name in header}
    issues = []

    for issue_name, columns in ROW_COMPLETENESS_RULES.items():
        if not all(column_name in col_idx for column_name in columns):
            continue

        bad_rows = []
        for row_number, row in enumerate(values[1:], start=2):
            row_values = {
                column_name: str(row[col_idx[column_name]]).strip()
                if col_idx[column_name] < len(row) and row[col_idx[column_name]] is not None
                else ""
                for column_name in columns
            }
            if any(row_values.values()) and not all(row_values.values()):
                missing_columns = [column_name for column_name, value in row_values.items() if not value]
                bad_rows.append({
                    "row_number": row_number,
                    "missing_columns": missing_columns,
                })

        if bad_rows:
            issues.append(
                {
                    "issue_name": issue_name,
                    "columns": columns,
                    "rows": bad_rows,
                }
            )

    return issues

def build_tracker_combined_data(spreadsheet, project_name=""):
    """Build the combined tracker data sheet and collect per-sheet issues.
    
    OPTIMIZATION (Flexible Column Approach):
    - Only uses REQUIRED_COLUMNS (9 essential columns) in merged output
    - Ignores extra/optional columns (Frame Count, Remarks, Error Count, etc.)
    - Detects required columns from the tracker header, even when columns are not contiguous
    - Reads only the required columns for row data
    - Reduces false format errors from shifted layouts
    """
    all_sheets = list_worksheets_with_retry(spreadsheet, project_name=project_name)
    combined_data = []
    standard_header = []
    header_added = False
    error_logs = []
    worksheet_values_map = {}

    for sheet in all_sheets:
        sheet_name = sheet.title.strip()
        if not VALID_TAB_QAI_REGEX.search(sheet_name):
            continue
        if sheet_name in {TRACKER_COMBINED_SHEET_NAME, TRACKER_DASHBOARD_SHEET_NAME}:
            continue

        values, full_header = read_tracker_required_values(sheet, project_name=project_name, sheet_name=sheet_name)

        worksheet_values_map[sheet_name] = {
            "values": values,
            "source_header": full_header,
        }
        
        merge_payload, merge_status, merge_header = extract_valid_tracker_rows_for_merge(
            values,
            source_header=full_header,
        )
        if merge_status != "OK":
            error_logs.append(
                {
                    "sheet": sheet_name,
                    "cell": "A1",
                    "message": f"Skipped from Project Data Collection: {merge_status}",
                }
            )
            continue

        for issue in collect_row_completeness_issues(values):
            first_row = issue["rows"][0]
            append_sheet_error_once(
                error_logs,
                sheet_name,
                f'{issue["issue_name"]} incomplete rows (missing: {", ".join(first_row["missing_columns"])})',
                header=values[0],
                col_name=first_row["missing_columns"][0] if first_row["missing_columns"] else issue["columns"][0],
                row_number=first_row["row_number"],
            )

        sheet_header = [str(v).strip() for v in values[0]]
        if not header_added:
            standard_header = REQUIRED_COLUMNS.copy()
            combined_data.append(standard_header)
            header_added = True

        task_name_index = standard_header.index("Annotation Task Name")
        annotator_index = standard_header.index("QAI ID And Annotator Name") if "QAI ID And Annotator Name" in standard_header else -1

        for row_number, row in enumerate(merge_payload["rows"], start=2):
            new_row = []

            for col_name in standard_header:
                idx_in_sheet = REQUIRED_COLUMNS.index(col_name) if col_name in REQUIRED_COLUMNS else -1
                cell_value = row[idx_in_sheet] if idx_in_sheet != -1 and idx_in_sheet < len(row) else ""
                # Sanitize cell value for UTF-8 safety
                new_row.append(sanitize_utf8(cell_value))

            task_name_value = new_row[task_name_index] if task_name_index < len(new_row) else ""
            if not str(task_name_value).strip():
                error_logs.append(
                    {
                        "sheet": sheet_name,
                        "cell": f"A{row_number}",
                        "message": 'Missing "Annotation Task Name"',
                    }
                )
                continue

            if annotator_index != -1:
                is_row_empty = not any(str(value).strip() for value in new_row)
                if not is_row_empty and not str(new_row[annotator_index]).strip():
                    error_logs.append(
                        {
                            "sheet": sheet_name,
                            "cell": f"{chr(65 + annotator_index)}{row_number}",
                            "message": 'Missing "QAI ID And Annotator Name"',
                        }
                    )

                new_row[annotator_index] = sheet_name

            combined_data.append(new_row)

    if not combined_data:
        return [], [], worksheet_values_map

    deduped_data = remove_duplicate_tracker_rows(combined_data, combined_data[0])
    cleaned_data = [deduped_data[0]]
    task_name_index = deduped_data[0].index("Annotation Task Name") if "Annotation Task Name" in deduped_data[0] else 0

    for row in deduped_data[1:]:
        if task_name_index < len(row) and str(row[task_name_index]).strip():
            cleaned_data.append(row)

    return cleaned_data, error_logs, worksheet_values_map

def build_dashboard_tables(data):
    """Create annotator and reviewer performance tables from merged tracker data.
    
    Operates on data that has been filtered to REQUIRED_COLUMNS only,
    providing faster dashboard generation and reliable metrics.
    """
    if not data or len(data) < 2:
        return None

    headers = data[0]
    annotator_index = headers.index("QAI ID And Annotator Name") if "QAI ID And Annotator Name" in headers else -1
    reviewer_index = headers.index("QAI ID And Reviewer Name") if "QAI ID And Reviewer Name" in headers else -1
    qc_verdict_index = headers.index("QC Verdict") if "QC Verdict" in headers else -1
    cc_verdict_index = headers.index("CC Verdict") if "CC Verdict" in headers else -1

    if annotator_index == -1 or reviewer_index == -1 or qc_verdict_index == -1 or cc_verdict_index == -1:
        return None

    annotator_map = {}
    reviewer_map = {}

    for row in data[1:]:
        if not row:
            continue

        annotator_full = row[annotator_index] if annotator_index < len(row) else ""
        reviewer_full = row[reviewer_index] if reviewer_index < len(row) else ""
        qc_verdict = str(row[qc_verdict_index] if qc_verdict_index < len(row) else "").strip().lower()
        cc_verdict = str(row[cc_verdict_index] if cc_verdict_index < len(row) else "").strip().lower()

        if annotator_full:
            annotator_stats = annotator_map.setdefault(
                annotator_full,
                {"task_count": 0, "correct": 0, "incorrect": 0},
            )
            annotator_stats["task_count"] += 1
            if qc_verdict == "correct":
                annotator_stats["correct"] += 1
            elif qc_verdict == "incorrect":
                annotator_stats["incorrect"] += 1

        if reviewer_full:
            reviewer_stats = reviewer_map.setdefault(
                reviewer_full,
                {"task_count": 0, "correct": 0, "incorrect": 0},
            )
            reviewer_stats["task_count"] += 1
            if cc_verdict == "correct":
                reviewer_stats["correct"] += 1
            elif cc_verdict == "incorrect":
                reviewer_stats["incorrect"] += 1

    annotator_table = [["Annotator ID", "Name", "Task Count", "Correct", "Incorrect", "Accuracy (%)"]]
    annotator_totals = {"task_count": 0, "correct": 0, "incorrect": 0}

    for raw_name, stats in sorted(annotator_map.items()):
        member_id, member_name = extract_dashboard_member_parts(raw_name)
        total_reviewed = stats["correct"] + stats["incorrect"]
        accuracy = round((stats["correct"] / total_reviewed) * 100, 2) if total_reviewed else 0
        annotator_table.append(
            [member_id, member_name, stats["task_count"], stats["correct"], stats["incorrect"], accuracy]
        )
        annotator_totals["task_count"] += stats["task_count"]
        annotator_totals["correct"] += stats["correct"]
        annotator_totals["incorrect"] += stats["incorrect"]

    annotator_total_reviewed = annotator_totals["correct"] + annotator_totals["incorrect"]
    annotator_total_accuracy = round((annotator_totals["correct"] / annotator_total_reviewed) * 100, 2) if annotator_total_reviewed else 0
    annotator_table.append(
        [
            "Total",
            "",
            annotator_totals["task_count"],
            annotator_totals["correct"],
            annotator_totals["incorrect"],
            annotator_total_accuracy,
        ]
    )

    reviewer_table = [["Reviewer ID", "Name", "Task Count", "CC Correct", "CC Incorrect", "Total CC", "Accuracy (%)"]]
    reviewer_totals = {"task_count": 0, "correct": 0, "incorrect": 0}

    for raw_name, stats in sorted(reviewer_map.items()):
        member_id, member_name = extract_dashboard_member_parts(raw_name)
        total_cc = stats["correct"] + stats["incorrect"]
        accuracy = round((stats["correct"] / total_cc) * 100, 2) if total_cc else 0
        reviewer_table.append(
            [member_id, member_name, stats["task_count"], stats["correct"], stats["incorrect"], total_cc, accuracy]
        )
        reviewer_totals["task_count"] += stats["task_count"]
        reviewer_totals["correct"] += stats["correct"]
        reviewer_totals["incorrect"] += stats["incorrect"]

    reviewer_total_cc = reviewer_totals["correct"] + reviewer_totals["incorrect"]
    reviewer_total_accuracy = round((reviewer_totals["correct"] / reviewer_total_cc) * 100, 2) if reviewer_total_cc else 0
    reviewer_table.append(
        [
            "Total",
            "",
            reviewer_totals["task_count"],
            reviewer_totals["correct"],
            reviewer_totals["incorrect"],
            reviewer_total_cc,
            reviewer_total_accuracy,
        ]
    )

    return {
        "annotator_table": annotator_table,
        "reviewer_table": reviewer_table,
    }

def build_dashboard_export_rows(project_name, tracker_url, dashboard_tables):
    """Convert generated dashboard tables into flat export rows for auditing."""
    annotation_out = []
    review_out = []

    if not dashboard_tables:
        return annotation_out, review_out

    for row in dashboard_tables["annotator_table"][1:]:
        if not row or str(row[0]).strip().lower() == "total":
            continue
        annotation_out.append(
            {
                "project_name": project_name,
                "tracker_url": tracker_url,
                "annotator_id": row[0],
                "resource_type": "Remote",
                "annotator_name": row[1],
                "task_count": row[2],
                "correct": row[3],
                "incorrect": row[4],
                "accuracy_pct": row[5],
            }
        )

    for row in dashboard_tables["reviewer_table"][1:]:
        if not row or str(row[0]).strip().lower() == "total":
            continue
        review_out.append(
            {
                "project_name": project_name,
                "tracker_url": tracker_url,
                "reviewer_id": row[0],
                "resource_type": "Remote",
                "reviewer_name": row[1],
                "task_count": row[2],
                "cc_correct": row[3],
                "cc_incorrect": row[4],
                "total_cc": row[5],
                "accuracy_pct": row[6],
            }
        )

    return annotation_out, review_out

def normalize_date_columns(df):
    """Normalize tracker date columns into a consistent display format."""
    date_columns = ['Annotation Date', 'QC Date']

    for col in date_columns:
        # Clean whitespace/newlines
        cleaned = (
            df[col]
            .astype(str)
            .str.strip()
            .str.replace(r'[\r\n]+', '', regex=True)
        )

        # Parse mixed formats
        # dayfirst=False because 5/4/2026 means Month/Date/Year
        datetime_series = pd.to_datetime(
            cleaned,
            errors='coerce',
            format='mixed',
            dayfirst=False
        )

        # Convert to Day-MonthName-Year
        df[col] = datetime_series.apply(
            lambda x: f"{x.day}-{x.strftime('%B')}-{x.year}"
            if pd.notnull(x) else None
        )

    return df

def build_datewise_summary(combined_data) -> pd.DataFrame:
    """Build a per-date annotation and review summary table for the tracker.
    
    Uses only REQUIRED_COLUMNS (9 essential columns) for fast processing.
    """
    cols_out = ["Date", "Annotator", "Task Count", "Reviewer", "Review Task Count"]

    if not isinstance(combined_data, list) or len(combined_data) < 2:
        return pd.DataFrame(columns=cols_out)

    header = [str(h).strip() for h in combined_data[0]]
    rows = combined_data[1:]

    try:
        df = pd.DataFrame(rows, columns=header)
    except Exception:
        df = pd.DataFrame(rows)
        df.columns = [header[i] if i < len(header) else f"col_{i}" for i in range(df.shape[1])]

    required = [
        "Annotation Task Name",
        "Annotation Date",
        "QAI ID And Annotator Name",
        "QC Date",
        "QAI ID And Reviewer Name",
    ]
    for c in required:
        if c not in df.columns:
            df[c] = ""

    df = normalize_date_columns(df)

    # Normalize / preserve blanks
    ann_date = df["Annotation Date"].fillna("").astype(str).str.strip()
    qc_date = df["QC Date"].fillna("").astype(str).str.strip()
    annotator = df["QAI ID And Annotator Name"].fillna("").astype(str).str.strip()
    reviewer = df["QAI ID And Reviewer Name"].fillna("").astype(str).str.strip()
    task_present = df["Annotation Task Name"].fillna("").astype(str).str.strip().astype(bool)

    working = pd.DataFrame({
        "Annotation Date": ann_date,
        "QC Date": qc_date,
        "Annotator": annotator,
        "Reviewer": reviewer,
        "TaskPresent": task_present,
    })

    # Annotation summary: ignore blank annotator names; count non-blank task entries
    ann = (
        working[working["Annotator"].astype(bool)]
        .groupby(["Annotation Date", "Annotator"], as_index=False)["TaskPresent"]
        .sum()
        .rename(columns={"Annotation Date": "Date", "Annotator": "Annotator", "TaskPresent": "Task Count"})
    )

    # Review summary: ignore blank reviewer names; count non-blank task entries
    rev = (
        working[working["Reviewer"].astype(bool)]
        .groupby(["QC Date", "Reviewer"], as_index=False)["TaskPresent"]
        .sum()
        .rename(columns={"QC Date": "Date", "Reviewer": "Reviewer", "TaskPresent": "Review Task Count"})
    )

    # Ensure deterministic ordering: non-blank dates first, then blank; names alphabetic
    ann = ann.sort_values(["Date", "Annotator"], key=lambda s: s.fillna("").astype(str)).reset_index(drop=True)
    rev = rev.sort_values(["Date", "Reviewer"], key=lambda s: s.fillna("").astype(str)).reset_index(drop=True)

    # Union of dates (deterministic)
    all_dates = sorted(
        set(ann["Date"].tolist() + rev["Date"].tolist()),
        key=lambda x: (x == "" or pd.isna(x), x),
    )

    # Pair entries per-date by index, padding shorter side with blanks
    out_rows = []
    for date in all_dates:
        a_grp = ann[ann["Date"] == date].reset_index(drop=True)
        r_grp = rev[rev["Date"] == date].reset_index(drop=True)
        max_len = max(len(a_grp), len(r_grp))
        for i in range(max_len):
            a_name = a_grp.loc[i, "Annotator"] if i < len(a_grp) else ""
            a_count = int(a_grp.loc[i, "Task Count"]) if i < len(a_grp) else ""
            r_name = r_grp.loc[i, "Reviewer"] if i < len(r_grp) else ""
            r_count = int(r_grp.loc[i, "Review Task Count"]) if i < len(r_grp) else ""
            out_rows.append([date, a_name, a_count, r_name, r_count])
    
    df = pd.DataFrame(out_rows, columns=cols_out)

    df["Annotator"] = df["Annotator"].apply(
        lambda x: split_member_id_and_name(x)[0]
    )

    df["Reviewer"] = df["Reviewer"].apply(
        lambda x: split_member_id_and_name(x)[0]
    )

    df = df.fillna("")

    return df.reset_index(drop=True)



def batch_highlight_errors(spreadsheet, error_logs):
    """Apply batched background formatting to issue cells across worksheets."""
    # Group cells by worksheet
    sheet_cells = defaultdict(list)

    for err in error_logs:
        sheet_cells[err["sheet"]].append(err["cell"])

    # Apply formatting in batches
    for sheet_name, cells in sheet_cells.items():
        try:
            ws = spreadsheet.worksheet(sheet_name)

            requests = []

            for cell in cells:
                requests.append({
                    "range": cell,
                    "format": {
                        "backgroundColor": {
                            "red": 1,
                            "green": 0.8,
                            "blue": 0.8
                        }
                    }
                })

            ws.batch_format(requests)

        except Exception as e:
            log(f"Failed formatting {sheet_name}: {e}")


def write_dashboard_error_table(dashboard_ws, spreadsheet, error_logs):
    """Write a compact error summary table into the generated dashboard sheet."""
    if error_logs:
        pivot_counts = {}
        for err in error_logs:
            key = f'{err["sheet"]} → {err["message"]}'
            pivot_counts.setdefault(key, {"count": 0, "sheet": err["sheet"]})
            pivot_counts[key]["count"] += err.get("count", 1)

        pivot_data = [["Sheet Name & Error Log", "Count"]]
        for key, value in pivot_counts.items():
            pivot_data.append([key, value["count"]])
            if HIGHLIGHT_TRACKER_ERRORS:
                try:
                    source_ws = spreadsheet.worksheet(value["sheet"])
                    set_sheet_tab_red(spreadsheet, source_ws)
                except Exception:
                    pass

        dashboard_ws.update(range_name="R1", values=pivot_data)
        format_header_row(dashboard_ws, 18, 2)

        if HIGHLIGHT_TRACKER_ERRORS:
            batch_highlight_errors(
                spreadsheet=spreadsheet,
                error_logs=error_logs
            )
    else:
        dashboard_ws.update(
            range_name="R1",
            values=[["Sheet Name & Error Log", "Count"], ["No Errors Found ✅", "0"]],
        )
        format_header_row(dashboard_ws, 18, 2)

def generate_tracker_dashboard(spreadsheet, project_name=""):
    """Generate helper sheets inside a tracker workbook and return summary stats."""
    combined_data, error_logs, worksheet_values_map = build_tracker_combined_data(
        spreadsheet,
        project_name=project_name,
    )

    combined_ws = clear_or_create_worksheet(spreadsheet, TRACKER_COMBINED_SHEET_NAME, rows=2000, cols=20)
    datewise_summary_ws = clear_or_create_worksheet(spreadsheet, TRACKER_DATEWISE_SUMMARY_SHEET_NAME, rows=2000, cols=20)

    if combined_data:
        combined_ws.update(range_name="A1", values=combined_data)
        format_header_row(combined_ws, 1, len(combined_data[0]))

        datewise_summary = build_datewise_summary(combined_data)

        if datewise_summary is not None and not datewise_summary.empty:
            values = [datewise_summary.columns.tolist()] + datewise_summary.astype(str).values.tolist()
            datewise_summary_ws.update(range_name="A1", values=values)
            format_header_row(datewise_summary_ws, 1, len(datewise_summary.columns))

    dashboard_ws = clear_or_create_worksheet(spreadsheet, TRACKER_DASHBOARD_SHEET_NAME, rows=1000, cols=20)

    dashboard_tables = build_dashboard_tables(combined_data)
    if dashboard_tables:
        annotator_table = dashboard_tables["annotator_table"]
        reviewer_table = dashboard_tables["reviewer_table"]

        dashboard_ws.update(range_name="A1", values=annotator_table)
        format_header_row(dashboard_ws, 1, len(annotator_table[0]))

        dashboard_ws.update(range_name="I1", values=reviewer_table)
        format_header_row(dashboard_ws, 9, len(reviewer_table[0]))

    write_dashboard_error_table(dashboard_ws, spreadsheet, error_logs)

    return {
        "combined_rows": max(len(combined_data) - 1, 0) if combined_data else 0,
        "error_count": len(error_logs),
        "dashboard_tables": dashboard_tables,
        "worksheet_values_map": worksheet_values_map,
    }

def normalize_qai_id(value):
    """Extract and normalize a tracker member identifier to `QAI_<suffix>`."""
    if value is None:
        return None

    text = str(value).strip()
    if not text:
        return None

    m = QAI_ID_REGEX.search(text)
    if not m:
        return None

    raw = m.group(1).upper()
    compact = re.sub(r"[^A-Z0-9]", "", raw)
    if not compact.startswith("QAI"):
        return None

    return "QAI_" + compact[3:]

def split_member_id_and_name(value):
    """Split a tracker member field into normalized QAI ID and cleaned name."""
    if value is None:
        return None, None

    raw = str(value).strip()
    if not raw:
        return None, None

    qai_id = normalize_qai_id(raw)
    name = raw

    if qai_id:
        name = QAI_ID_REGEX.sub("", name)
        name = re.sub(r"[\(\)\[\]\-_,:|/]+", " ", name)
        name = re.sub(r"\s+", " ", name).strip()

    if not name:
        name = None

    return qai_id, name

def strip_qai_id_from_member_text(value):
    """Remove a QAI identifier from free-form member text and keep the name."""
    if value is None:
        return None

    raw = str(value).strip()
    if not raw:
        return None

    name = QAI_ID_REGEX.sub("", raw)
    name = re.sub(r"^[\s\(\)\[\]\-_,:|/]+|[\s\(\)\[\]\-_,:|/]+$", "", name)
    name = re.sub(r"\s+", " ", name).strip()

    return name or raw

def _resolve_lookup_columns(header):
    """Resolve flexible resource lookup headers for ID and resource type columns."""
    normalized = [
        re.sub(r"[^a-z0-9]+", " ", str(h).strip().lower()).strip()
        for h in header
    ]

    id_candidates = {"qai_id", "qai id", "resource id", "employee id", "id"}
    type_candidates = {"designation", "resource_type", "resource type", "type"}

    id_idx = None
    type_idx = None

    for i, col in enumerate(normalized):
        if col in id_candidates and id_idx is None:
            id_idx = i
        if col in type_candidates and type_idx is None:
            type_idx = i

    # Flexible fallback for custom headers such as "QAI ID And Name"
    if id_idx is None:
        for i, col in enumerate(normalized):
            if "qai" in col and "id" in col:
                id_idx = i
                break

    if type_idx is None:
        for i, col in enumerate(normalized):
            if any(k in col for k in ["designation", "resource type", "resource_type", "type"]):
                type_idx = i
                break

    return id_idx, type_idx

def load_resource_type_lookup():
    """Load the resource-type lookup table.

    The current implementation intentionally returns an empty mapping, which
    causes members to default to `Remote`. Keep this function as the extension
    point for future enrichment from a lookup tab or separate sheet.
    """
    return {}

# ============================================================
# DASHBOARD PARSING
# ============================================================

def _normalize_cell(value):
    """Normalize arbitrary cell text for resilient dashboard parsing."""
    return re.sub(r"[^a-z0-9]+", " ", str(value).strip().lower()).strip()

def _find_header_row(values, required_phrases):
    """Find the first row that contains all required header phrases."""
    for i, row in enumerate(values):
        normalized_cells = [_normalize_cell(c) for c in row if str(c).strip()]
        if not normalized_cells:
            continue
        if all(any(req in cell for cell in normalized_cells) for req in required_phrases):
            return i
    return None

def _find_col_index(header_row, phrases):
    """Find the first column whose normalized header matches any phrase."""
    normalized = [_normalize_cell(c) for c in header_row]
    for phrase in phrases:
        for i, cell in enumerate(normalized):
            if phrase in cell:
                return i
    return None

def _find_col_index_near(header_row, phrases, anchor_idx, direction="right"):
    """Find a matching column near an anchor column in the dashboard sheet."""
    normalized = [_normalize_cell(c) for c in header_row]
    candidates = []
    for phrase in phrases:
        for i, cell in enumerate(normalized):
            if phrase in cell:
                candidates.append(i)

    if not candidates:
        return None

    if anchor_idx is None:
        return min(candidates)

    if direction == "right":
        right = [i for i in candidates if i >= anchor_idx]
        if right:
            return min(right)
    elif direction == "left":
        left = [i for i in candidates if i <= anchor_idx]
        if left:
            return max(left)

    # Fallback to nearest
    return min(candidates, key=lambda i: abs(i - anchor_idx))

def _parse_table(values, header_idx, col_map):
    """Parse a rectangular table using a fixed header row and column map."""
    if header_idx is None:
        return []

    header = values[header_idx]
    col_idx = {}
    for key, phrases in col_map.items():
        idx = _find_col_index(header, phrases)
        if idx is None:
            return []
        col_idx[key] = idx

    rows = []
    empty_streak = 0
    for r in values[header_idx + 1 :]:
        row_values = {k: (r[i] if i < len(r) else "") for k, i in col_idx.items()}
        normalized_first = _normalize_cell(row_values.get("id", ""))
        if normalized_first == "total":
            break

        if not any(str(v).strip() for v in row_values.values()):
            empty_streak += 1
            if empty_streak >= 2:
                break
            continue

        empty_streak = 0
        rows.append(row_values)

    return rows

def _parse_table_with_anchor(values, header_idx, anchor_phrases, col_map):
    """Parse a table whose target columns are positioned near an anchor header."""
    if header_idx is None:
        return []

    header = values[header_idx]
    anchor_idx = _find_col_index(header, anchor_phrases)
    if anchor_idx is None:
        return []

    col_idx = {}
    for key, phrases in col_map.items():
        idx = _find_col_index_near(header, phrases, anchor_idx, direction="right")
        if idx is None:
            return []
        col_idx[key] = idx

    rows = []
    empty_streak = 0
    for r in values[header_idx + 1 :]:
        row_values = {k: (r[i] if i < len(r) else "") for k, i in col_idx.items()}
        normalized_first = _normalize_cell(row_values.get("id", ""))
        if normalized_first == "total":
            break

        if not any(str(v).strip() for v in row_values.values()):
            empty_streak += 1
            if empty_streak >= 2:
                break
            continue

        empty_streak = 0
        rows.append(row_values)

    return rows

def extract_dashboard(values, project_name, tracker_url):
    """Parse an existing tracker dashboard sheet into flat export rows."""
    if not values:
        return [], []

    annot_header_idx = _find_header_row(values, ["annotator id", "task count", "accuracy"])
    review_header_idx = _find_header_row(values, ["reviewer id", "cc", "accuracy"])

    annot_rows = _parse_table_with_anchor(
        values,
        annot_header_idx,
        ["annotator id"],
        {
            "id": ["annotator id"],
            "name": ["name"],
            "task_count": ["task count"],
            "correct": ["correct"],
            "incorrect": ["incorrect"],
            "accuracy_pct": ["accuracy"],
        },
    )

    review_rows = _parse_table_with_anchor(
        values,
        review_header_idx,
        ["reviewer id"],
        {
            "id": ["reviewer id"],
            "name": ["name"],
            "task_count": ["task count"],
            "cc_correct": ["cc correct"],
            "cc_incorrect": ["cc incorrect"],
            "total_cc": ["total cc"],
            "accuracy_pct": ["accuracy"],
        },
    )

    annotation_out = []
    for row in annot_rows:
        annotator_id = row.get("id", "")
        annotator_qai = normalize_qai_id(annotator_id)
        annotation_out.append(
            {
                "project_name": project_name,
                "tracker_url": tracker_url,
                "annotator_id": annotator_id,
                "resource_type": resource_type_map.get(annotator_qai, "Remote")
                if annotator_qai
                else "Remote",
                "annotator_name": row.get("name", ""),
                "task_count": row.get("task_count", ""),
                "correct": row.get("correct", ""),
                "incorrect": row.get("incorrect", ""),
                "accuracy_pct": row.get("accuracy_pct", ""),
            }
        )

    review_out = []
    for row in review_rows:
        reviewer_id = row.get("id", "")
        reviewer_qai = normalize_qai_id(reviewer_id)
        review_out.append(
            {
                "project_name": project_name,
                "tracker_url": tracker_url,
                "reviewer_id": reviewer_id,
                "resource_type": resource_type_map.get(reviewer_qai, "Remote")
                if reviewer_qai
                else "Remote",
                "reviewer_name": row.get("name", ""),
                "task_count": row.get("task_count", ""),
                "cc_correct": row.get("cc_correct", ""),
                "cc_incorrect": row.get("cc_incorrect", ""),
                "total_cc": row.get("total_cc", ""),
                "accuracy_pct": row.get("accuracy_pct", ""),
            }
        )

    return annotation_out, review_out

def convert_to_number_or_blank(value):
    """Return a numeric value when possible, otherwise keep the field blank."""
    if isinstance(value, str):
        value = value.strip()
        
        if value.startswith("="):
            return ""
    
    try:
        return float(value)
    
    except Exception:
        return ""

# ============================================================
# MAIN EXTRACTION
# ============================================================

def build_stage_count(df, date_col, member_col, out_col):
    """Count unique members working per project and date for one stage."""
    # Validate required columns exist
    required_cols = ["project_name", date_col, member_col]
    for col in required_cols:
        if col not in df.columns:
            log(f"⚠️ Missing column '{col}' in stage data")
            return pd.DataFrame(columns=["project_name", "date", out_col])
    
    stage = df[["project_name", date_col, member_col]].dropna(subset=[date_col, member_col]).copy()

    if stage.empty:
        return pd.DataFrame(columns=["project_name", "date", out_col])

    member_info = stage[member_col].apply(split_member_id_and_name)
    stage["qai_id"] = member_info.apply(lambda x: x[0])
    stage["member_name_in_tracker"] = stage[member_col].astype(str).str.strip()
    stage["member_key"] = stage["qai_id"].fillna(stage["member_name_in_tracker"])

    grouped = (
        stage.groupby(["project_name", date_col], as_index=False)["member_key"]
        .nunique()
        .rename(columns={date_col: "date", "member_key": out_col})
    )

    return grouped

def build_member_activity(df, date_col, member_col, work_stage):
    """Build a normalized member activity table for one workflow stage."""
    # Validate required columns exist
    required_cols = ["project_name", date_col, member_col]
    for col in required_cols:
        if col not in df.columns:
            log(f"⚠️ Missing column '{col}' in member activity data")
            return pd.DataFrame(
                columns=[
                    "date",
                    "project_name",
                    "work_stage",
                    "qai_id",
                    "member_name",
                    "member_name_in_tracker",
                    "resource_type",
                ]
            )
    
    stage = df[["project_name", date_col, member_col]].dropna(subset=[date_col, member_col]).copy()

    if stage.empty:
        return pd.DataFrame(
            columns=[
                "date",
                "project_name",
                "work_stage",
                "qai_id",
                "member_name",
                "member_name_in_tracker",
                "resource_type",
            ]
        )

    member_info = stage[member_col].apply(split_member_id_and_name)
    stage["qai_id"] = member_info.apply(lambda x: x[0])
    stage["member_name_in_tracker"] = stage[member_col].astype(str).str.strip()
    stage["member_name"] = stage["member_name_in_tracker"].apply(strip_qai_id_from_member_text)
    stage["resource_type"] = stage["qai_id"].map(resource_type_map).fillna("Remote")
    stage["work_stage"] = work_stage

    stage = stage.rename(columns={date_col: "date"})[
        [
            "date",
            "project_name",
            "work_stage",
            "qai_id",
            "member_name",
            "member_name_in_tracker",
            "resource_type",
        ]
    ]

    return stage

def count_column_entries(values, column_name):
    """Count non-empty entries in a specific column across all rows"""
    if not values or len(values) < 1:
        return 0
    
    header = values[0]
    try:
        col_idx = header.index(column_name)
    except ValueError:
        return 0  # Column doesn't exist
    
    count = 0
    for row in values[1:]:
        if col_idx < len(row) and str(row[col_idx]).strip():
            count += 1
    
    return count

def build_project_summary(tasks, gs_client):
    """Build a per-project sheet summary by scanning tracker worksheet contents."""
    summary_rows = []
    current_date = datetime.now(timezone.utc).date()
    
    for task in tasks:
        project_name = task.get("name")
        tracker_url = None
        data_quantity = ""
        
        for f in task.get("custom_fields", []):
            if f.get("name") == TRACKER_FIELD_NAME:
                tracker_url = f.get("value")
            if f.get("name") == DATA_QUANTITY_FIELD_NAME:
                data_quantity = f.get("value") or ""
        
        if not tracker_url:
            continue
        
        m = re.search(r"/d/([a-zA-Z0-9-_]+)", str(tracker_url))
        if not m:
            continue
        
        sheet_id = m.group(1)
        
        try:
            spreadsheet = open_spreadsheet_by_key_with_retry(gs_client, sheet_id, project_name=project_name)
        except Exception:
            continue
        
        annotation_count = 0
        qa_count = 0
        cc_count = 0
        
        # Scan ALL sheets
        try:
            worksheets = list_worksheets_with_retry(spreadsheet, project_name=project_name)
        except Exception:
            continue

        for ws in worksheets:
            try:
                time.sleep(READ_DELAY)
                values = ws.get("A:Z")
                
                annotation_count += count_column_entries(values, "Annotation Task Name")
                qa_count += count_column_entries(values, "QAI ID And Reviewer Name")
                cc_count += count_column_entries(values, "QAI ID And Cross Checker Name")
            except Exception:
                continue
        
        summary_rows.append({
            "project_name": project_name,
            "deliverables": convert_to_number_or_blank(data_quantity),
            "date": str(current_date),
            "annotation completed": annotation_count,
            "QA completed": qa_count,
            "CC completed": cc_count,
        })
    
    return pd.DataFrame(summary_rows)

def reset_run_state():
    """Clear global in-memory log containers before a fresh pipeline run."""
    tracker_format_issue_logs.clear()
    tab_audit_logs.clear()
    project_status_map.clear()
    dashboard_annotation_rows.clear()
    dashboard_review_rows.clear()
    project_run_logs.clear()

def main():
    """Execute the full ClickUp-to-Google-Sheets audit pipeline."""
    global resource_type_map

    load_runtime_config()
    authenticate_google()
    reset_run_state()

    tasks = filter_tasks_for_test_run(fetch_clickup_tasks())
    
    try:
        resource_type_map = load_resource_type_lookup()
    except Exception as e:
        log(f"⚠️ Failed to load resource type lookup (using defaults): {e}")
        resource_type_map = {}

    total_projects = len(tasks)
    projects_with_tracker = 0
    projects_processed = set()
    projects_failed = set()

    all_dfs = []
    project_data_quantity = {}

    for index, task in enumerate(tqdm(tasks, desc="Processing Projects"), start=1):
        project_name = task.get("name")
        log(f"▶️ Processing project {index}/{total_projects}: {project_name}")
        project_status_map[project_name] = "NO_VALID_DATA"

        tracker_url = None
        data_type = ""
        delivery_lead_email = ""
        data_quantity = ""

        for f in task.get("custom_fields", []):
            if f.get("name") == TRACKER_FIELD_NAME:
                tracker_url = f.get("value")
            if f.get("name") == "Data Type":
                data_type = f.get("value") or ""
            if f.get("name") == DATA_QUANTITY_FIELD_NAME:
                data_quantity = f.get("value") or ""

        delivery_lead_email = get_task_delivery_lead_email(task)

        data_quantity = convert_to_number_or_blank(data_quantity)
        project_data_quantity[project_name] = data_quantity
        project_log = build_project_log_row(
            task,
            tracker_url=tracker_url,
            data_type=data_type,
            delivery_lead_email=delivery_lead_email,
            data_quantity=data_quantity,
        )

        if not tracker_url:
            log(f"⏭️ Skipping {project_name}: no tracker URL found")
            project_status_map[project_name] = "NO_TRACKER"
            finalize_project_log(project_log, "NO_TRACKER")
            continue

        projects_with_tracker += 1

        m = re.search(r"/d/([a-zA-Z0-9-_]+)", str(tracker_url))
        if not m:
            log(f"⏭️ Skipping {project_name}: invalid tracker URL")
            project_status_map[project_name] = "INVALID_TRACKER_URL"
            finalize_project_log(project_log, "INVALID_TRACKER_URL")
            continue

        sheet_id = m.group(1)
        project_log["tracker_sheet_id"] = sheet_id

        try:
            spreadsheet = open_spreadsheet_by_key_with_retry(gs_client, sheet_id, project_name=project_name)
        except Exception:
            projects_failed.add(project_name)
            log(f"❌ Failed to open tracker for {project_name}")
            project_status_map[project_name] = "SHEET_OPEN_FAILED"
            finalize_project_log(project_log, "SHEET_OPEN_FAILED")
            continue

        try:
            tracker_dashboard_result = generate_tracker_dashboard(spreadsheet, project_name=project_name)
            worksheet_values_map = tracker_dashboard_result.get("worksheet_values_map", {})
            dashboard_tables = tracker_dashboard_result.get("dashboard_tables")

            annot_rows, review_rows = build_dashboard_export_rows(
                project_name,
                tracker_url,
                dashboard_tables,
            )
            if annot_rows:
                dashboard_annotation_rows.extend(annot_rows)
            if review_rows:
                dashboard_review_rows.extend(review_rows)
            project_log["dashboard_tabs_seen"] += 1
            project_log["dashboard_rows_found"] += len(annot_rows) + len(review_rows)

            log_tab_activity(
                project_name,
                tracker_url,
                TRACKER_DASHBOARD_SHEET_NAME,
                "GENERATED",
                f'Combined rows: {tracker_dashboard_result["combined_rows"]}, errors: {tracker_dashboard_result["error_count"]}',
                delivery_lead_email,
                data_type,
                total_rows=tracker_dashboard_result["combined_rows"],
            )
        except Exception as e:
            worksheet_values_map = {}
            log_tab_activity(
                project_name,
                tracker_url,
                TRACKER_DASHBOARD_SHEET_NAME,
                "ERROR",
                f"DASHBOARD_GENERATION_FAILED: {e}",
                delivery_lead_email,
                data_type,
            )
            log(f"⚠️ Dashboard generation failed for {project_name}: {e}")

        project_had_data = False
        last_project_issue_reason = ""
        try:
            worksheets = list_worksheets_with_retry(spreadsheet, project_name=project_name)
        except Exception:
            projects_failed.add(project_name)
            project_status_map[project_name] = "READ_QUOTA_FAILED"
            project_log["last_issue_reason_code"] = "WORKSHEET_METADATA_QUOTA_EXCEEDED"
            last_project_issue_reason = "Worksheet metadata could not be loaded because quota retries were exhausted."
            finalize_project_log(project_log, "READ_QUOTA_FAILED", last_project_issue_reason)
            log(f"⚠️ Finished {project_name} with status: READ_QUOTA_FAILED")
            continue

        project_log["total_worksheets"] = len(worksheets)

        for ws in worksheets:
            sheet_name = ws.title.strip()

            if sheet_name == "Dashboard":
                continue

            if sheet_name in SKIP_SHEETS:
                continue

            if not VALID_TAB_QAI_REGEX.search(sheet_name):
                continue

            project_log["eligible_qai_tabs"] += 1

            worksheet_snapshot = worksheet_values_map.get(sheet_name)
            if worksheet_snapshot is None:
                try:
                    values, full_header = read_tracker_required_values(
                        ws,
                        project_name=project_name,
                        sheet_name=sheet_name,
                    )
                    worksheet_snapshot = {
                        "values": values,
                        "source_header": full_header,
                    }
                except Exception:
                    projects_failed.add(project_name)
                    project_status_map[project_name] = "READ_QUOTA_FAILED"
                    project_log["tabs_errored"] += 1
                    project_log["last_issue_tab"] = sheet_name
                    project_log["last_issue_reason_code"] = "READ_QUOTA_EXCEEDED"
                    last_project_issue_reason = "One or more eligible QAI tabs could not be read because quota retries were exhausted."
                    log_tab_activity(
                        project_name,
                        tracker_url,
                        sheet_name,
                        "ERROR",
                        "READ_QUOTA_EXCEEDED",
                        delivery_lead_email,
                        data_type,
                    )
                    continue
            values = worksheet_snapshot.get("values", [])
            source_header = worksheet_snapshot.get("source_header", values[0] if values else [])

            if not values:
                project_log["tabs_skipped"] += 1
                project_log["last_issue_tab"] = sheet_name
                project_log["last_issue_reason_code"] = "EMPTY_TAB"
                log_tracker_format_issue(
                    project_name,
                    tracker_url,
                    sheet_name,
                    "ISSUE_EMPTY_TAB",
                    delivery_lead_email=delivery_lead_email,
                    data_type=data_type,
                )
                last_project_issue_reason = "Eligible QAI tabs were found, but at least one tab was empty."
                log_tab_activity(
                    project_name,
                    tracker_url,
                    sheet_name,
                    "SKIPPED",
                    "EMPTY_TAB",
                    delivery_lead_email,
                    data_type,
                )
                continue

            header = [str(h).strip() for h in source_header[:20]]
            is_valid_format, matched_format, mismatches = validate_tracker_header(header)
            
            if is_valid_format:
                log(f"\u2705 Valid header for {sheet_name} (has all required columns)")
                log_tracker_format_issue(
                    project_name,
                    tracker_url,
                    sheet_name,
                    "OK",
                    matched_format=matched_format,
                    actual_header=" | ".join(header),
                    delivery_lead_email=delivery_lead_email,
                    data_type=data_type,
                )
            else:
                log(f"\u26a0\ufe0f  {sheet_name} missing some columns: {', '.join(m['expected'] for m in mismatches[:3])}")
                mismatch_text = "; ".join(
                    f"{m['column_letter']}: expected '{m['expected']}' but found '{m['actual']}'"
                    for m in mismatches
                )
                log_tracker_format_issue(
                    project_name,
                    tracker_url,
                    sheet_name,
                    "ISSUE_INVALID_REQUIRED_COLUMNS",
                    matched_format=matched_format,
                    mismatch_count=len(mismatches),
                    mismatch_details=mismatch_text,
                    actual_header=" | ".join(header),
                    delivery_lead_email=delivery_lead_email,
                    data_type=data_type,
                )
                project_log["tabs_skipped"] += 1
                project_log["last_issue_tab"] = sheet_name
                project_log["last_issue_reason_code"] = "INVALID_REQUIRED_COLUMNS"
                last_project_issue_reason = "Eligible QAI tabs were skipped because one or more required headers were missing."
                log_tab_activity(
                    project_name,
                    tracker_url,
                    sheet_name,
                    "SKIPPED",
                    "INVALID_REQUIRED_COLUMNS",
                    delivery_lead_email,
                    data_type,
                    total_rows=max(len(values) - 1, 0),
                )
                continue

            for issue in collect_row_completeness_issues(values):
                sample_rows = ", ".join(str(item["row_number"]) for item in issue["rows"][:10])
                missing_patterns = "; ".join(
                    f'row {item["row_number"]}: missing {", ".join(item["missing_columns"])}'
                    for item in issue["rows"][:5]
                )
                log_tracker_format_issue(
                    project_name,
                    tracker_url,
                    sheet_name,
                    f'ISSUE_{issue["issue_name"]}_INCOMPLETE_ROWS',
                    mismatch_count=len(issue["rows"]),
                    mismatch_details=missing_patterns,
                    actual_header=" | ".join(header),
                    delivery_lead_email=delivery_lead_email,
                    data_type=data_type,
                )
                log_tab_activity(
                    project_name,
                    tracker_url,
                    sheet_name,
                    "WARN",
                    f'{issue["issue_name"]}_INCOMPLETE_ROWS: {len(issue["rows"])} row(s); sample rows: {sample_rows}',
                    delivery_lead_email,
                    data_type,
                    total_rows=max(len(values) - 1, 0),
                )

            if len(values) < 2:
                project_log["tabs_skipped"] += 1
                project_log["last_issue_tab"] = sheet_name
                project_log["last_issue_reason_code"] = "EMPTY_TAB"
                last_project_issue_reason = "Eligible QAI tabs were found, but at least one tab was empty."
                log_tab_activity(
                    project_name,
                    tracker_url,
                    sheet_name,
                    "SKIPPED",
                    "EMPTY_TAB",
                    delivery_lead_email,
                    data_type,
                )
                continue

            projected_header = [str(h).strip() for h in values[0][:20]]
            available_col_idx = {c: projected_header.index(c) for c in REQUIRED_COLUMNS if c in projected_header}
            supported_stages = [
                stage_name
                for stage_name, stage_cols in STAGE_REQUIRED_COLUMNS.items()
                if all(col in available_col_idx for col in stage_cols)
            ]
            missing_cols = [c for c in REQUIRED_COLUMNS if c not in available_col_idx]

            if not supported_stages:
                project_log["tabs_skipped"] += 1
                project_log["last_issue_tab"] = sheet_name
                project_log["last_issue_reason_code"] = "NO_SUPPORTED_STAGE_COLUMNS"
                last_project_issue_reason = "Eligible QAI tabs were skipped because no annotation, QC, or CC stage columns were available."
                log_tab_activity(
                    project_name,
                    tracker_url,
                    sheet_name,
                    "SKIPPED",
                    f"NO_SUPPORTED_STAGE_COLUMNS: {missing_cols}",
                    delivery_lead_email,
                    data_type,
                    total_rows=len(values) - 1,
                )
                continue

            rows = []

            for r in values[1:]:
                if not any(str(x).strip() for x in r):
                    continue
                row_values = [
                    r[available_col_idx[c]] if c in available_col_idx and available_col_idx[c] < len(r) else None
                    for c in REQUIRED_COLUMNS
                ]
                has_stage_data = any(
                    any(
                        str(row_values[REQUIRED_COLUMNS.index(col)]).strip()
                        for col in stage_cols
                        if row_values[REQUIRED_COLUMNS.index(col)] is not None
                    )
                    for stage_cols in STAGE_REQUIRED_COLUMNS.values()
                )
                if not has_stage_data:
                    continue
                rows.append(row_values)

            if not rows:
                project_log["tabs_skipped"] += 1
                project_log["last_issue_tab"] = sheet_name
                project_log["last_issue_reason_code"] = "NO_VALID_ROWS"
                last_project_issue_reason = "Eligible QAI tabs were found, but they did not contain valid data rows."
                log_tab_activity(
                    project_name,
                    tracker_url,
                    sheet_name,
                    "SKIPPED",
                    "NO_VALID_ROWS",
                    delivery_lead_email,
                    data_type,
                    total_rows=len(values) - 1,
                )
                continue

            df = pd.DataFrame(rows, columns=REQUIRED_COLUMNS)
            df["project_name"] = sanitize_utf8(project_name)
            df["Data Type"] = sanitize_utf8(data_type)
            df["Delivery Lead Email"] = sanitize_utf8(delivery_lead_email)
            
            # Sanitize all string columns in the DataFrame
            for col in df.columns:
                if df[col].dtype == 'object':
                    df[col] = df[col].apply(sanitize_utf8)

            all_dfs.append(df)
            project_had_data = True
            projects_processed.add(project_name)
            project_log["tabs_added"] += 1
            project_log["compiled_rows_added"] += len(df)
            project_log["last_successful_tab"] = sheet_name

            log_tab_activity(
                project_name,
                tracker_url,
                sheet_name,
                "ADDED",
                "SUCCESS",
                delivery_lead_email,
                data_type,
                total_rows=len(values) - 1,
                rows_added=len(df),
            )

        if not project_had_data:
            if project_status_map[project_name] != "READ_QUOTA_FAILED":
                project_status_map[project_name] = "NO_VALID_ROWS"
            if not last_project_issue_reason:
                if project_log["eligible_qai_tabs"] == 0:
                    last_project_issue_reason = "No eligible QAI tabs were found in the tracker."
                else:
                    last_project_issue_reason = PROJECT_STATUS_REASON_MAP[project_status_map[project_name]]
            if project_status_map[project_name] == "NO_VALID_ROWS":
                log_tracker_format_issue(
                    project_name,
                    tracker_url,
                    "PROJECT_LEVEL",
                    "PROJECT_NOT_STARTED_OR_TRACKER_IS_EMPTY",
                    mismatch_count=1,
                    mismatch_details=NO_DATA_FETCHED_REASON,
                    actual_header="",
                    delivery_lead_email=delivery_lead_email,
                    data_type=data_type,
                )
                log_tab_activity(
                    project_name,
                    tracker_url,
                    "PROJECT_LEVEL",
                    "WARN",
                    NO_DATA_FETCHED_REASON,
                    delivery_lead_email,
                    data_type,
                    total_rows=0,
                )
            finalize_project_log(project_log, project_status_map[project_name], last_project_issue_reason)
            log(f"⚠️ Finished {project_name} with status: {project_status_map[project_name]}")
        else:
            project_status_map[project_name] = "PROCESSED_SUCCESS"
            finalize_project_log(project_log, project_status_map[project_name])
            log(f"✅ Finished {project_name} with status: {project_status_map[project_name]}")

    project_run_log_df = pd.DataFrame(project_run_logs, columns=PROJECT_RUN_LOG_COLUMNS).sort_values(
        ["compiled_into_report", "project_name"],
        ascending=[True, True],
    ).reset_index(drop=True)

    # Write audit data with error handling to prevent data loss
    audit_writes = [
        (project_run_log_df, "Pipeline Execution Logs"),
        (pd.DataFrame(tab_audit_logs), "Project Tracker Audit Log"),
        (pd.DataFrame(tracker_format_issue_logs), "Task Tracker Format Issues"),
    ]
    
    for df_to_upload, tab_name in audit_writes:
        try:
            log(f"📝 Writing audit data to '{tab_name}'...")
            upload(df_to_upload, audit_sheet, tab_name)
            log(f"✅ Successfully wrote to '{tab_name}'")
        except Exception as e:
            log(f"❌ FAILED to write to {tab_name}: {e}")
            log(f"⚠️  WARNING: Data for {tab_name} may be incomplete or lost!")
            # Continue trying to write remaining tabs instead of crashing
            continue

    if not all_dfs:
        log("⚠️ No data extracted.")
    else:
        # Save compiled data locally as backup (prevents data loss if final writes fail)
        try:
            backup_df = pd.concat(all_dfs, ignore_index=True)
            backup_file = f"compiled_data_backup_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}.csv"
            backup_df.to_csv(backup_file, index=False)
            log(f"💾 Backup saved: {backup_file} ({len(backup_df)} rows)")
        except Exception as e:
            log(f"⚠️ Failed to save backup: {e}")

    log("===================================")
    log("PIPELINE SUMMARY")
    log(f"Total ClickUp projects: {total_projects}")
    log(f"Projects with tracker: {projects_with_tracker}")
    log(f"Projects successfully processed: {len(projects_processed)}")
    log(f"Projects failed: {len(projects_failed)}")
    log("----- Project Status Breakdown -----")

    for k, v in project_status_map.items():
        log(f"{k} -> {v}")

    log("===================================")
    log("🎉 PIPELINE COMPLETED SUCCESSFULLY")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        log(f"❌ Pipeline failed with error: {e}")
        reset_run_state()
        sys.exit(1)
    finally:
        reset_run_state()
