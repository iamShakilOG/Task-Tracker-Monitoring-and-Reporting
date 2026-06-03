# Setup Instructions

## Prerequisites
- Python 3.9 or higher
- pip (Python package manager)
- Google service account credentials (JSON key file)
- ClickUp API token

## Installation

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd clean_task_tracker_etl
   ```

2. **Create a Python virtual environment** (recommended)
   ```bash
   python -m venv venv
   source venv/Scripts/activate  # On Windows
   # or: source venv/bin/activate  # On macOS/Linux
   ```

3. **Install dependencies**
   ```bash
   pip install -r requirements.txt
   ```

## Configuration

1. **Copy the example environment file**
   ```bash
   cp .env.example .env
   ```

2. **Update `.env` with your credentials**
   - `CLICKUP_API_TOKEN`: Get from ClickUp account settings
   - `GOOGLE_SERVICE_ACCOUNT_JSON`: Path to your Google service account JSON key
   - `AUDIT_SHEET_URL`: Google Sheet URL for audit logs
   - `PROJECT_NAME_FILTER`: Comma-separated project names to ignore
   - `IGNORE_PDL_EMAILS`: Comma-separated PDL emails to ignore

3. **Place Google service account JSON file**
   - Save your `service_account.json` in the repository root
   - Never commit this file to version control (it's in `.gitignore`)

## Running the Pipeline

```bash
python task_tracker_issue_audit_pipeline.py
```

## What the Script Does

- Fetches active projects from ClickUp
- Filters projects by name and PDL email
- Validates Google Sheets tracker tabs
- Generates performance dashboards
- Writes comprehensive audit logs

## Troubleshooting

**"CLICKUP_API_TOKEN not found"**
- Ensure `.env` file exists with `CLICKUP_API_TOKEN` set

**"service_account.json not found"**
- Verify Google service account JSON file is in repo root
- Update `GOOGLE_SERVICE_ACCOUNT_JSON` path in `.env` if needed

**Google authentication errors**
- Ensure service account has access to Google Sheets
- Verify `AUDIT_SHEET_URL` is correct and accessible
