# Task Tracker Issue Audit Pipeline

This project runs a production ETL that:

- reads project trackers from ClickUp
- validates eligible tracker tabs in Google Sheets
- generates `Project Data Collection`, `Dashboard`, and `Datewise Summary` inside each tracker
- writes run logs, tab audit logs, and tracker format issues into a central audit sheet

## Files

- [task_tracker_issue_audit_pipeline.py](/mnt/e/Scripts/ETLS_2026/clean_task_tracker_etl/task_tracker_issue_audit_pipeline.py) is the main pipeline entrypoint.
- [requirements.txt](/mnt/e/Scripts/ETLS_2026/clean_task_tracker_etl/requirements.txt) contains Python dependencies.
- [`.github/workflows/task-tracker-issue-audit.yml`](/mnt/e/Scripts/ETLS_2026/clean_task_tracker_etl/.github/workflows/task-tracker-issue-audit.yml) runs the pipeline on GitHub Actions every day.

## Required Environment Variables

Set these locally in `.env` or in GitHub repository secrets:

- `CLICKUP_API_TOKEN`: ClickUp API token used to read the configured list.
- `GOOGLE_SERVICE_ACCOUNT_JSON`: Google service account JSON. This can be:
  - raw JSON
  - base64-encoded JSON
  - a local file path when running locally
- `AUDIT_SHEET_URL`: Google Sheets URL for the central audit workbook.

Optional variables:

- `PROJECT_NAME_FILTER`: Optional ignore list for exact project names. Supports one or many names separated by commas or new lines.
- `IGNORE_PDL_EMAILS`: Optional list of PDL emails to skip entirely. Supports commas or new lines. The pipeline reads either the `PDL Email` or `Delivery Lead Email` ClickUp custom field.
- `HIGHLIGHT_TRACKER_ERRORS`: `true` or `false`. Defaults to `true`.
- `RESOURCE_LOOKUP_TAB`: Optional sheet tab name reserved for future resource-type mapping. Defaults to `Team List & Activity`.

Examples:

```env
PROJECT_NAME_FILTER=Project Alpha,Project Beta
IGNORE_PDL_EMAILS=pdl1@example.com,pdl2@example.com
```

## Local Run

Install dependencies:

```bash
pip install -r requirements.txt
```

Run the pipeline:

```bash
python3 task_tracker_issue_audit_pipeline.py
```

## GitHub Actions Schedule

GitHub Actions cron uses UTC, not GMT+6.

If you want the workflow to run every hour, the correct cron is:

```yaml
schedule:
  - cron: "0 * * * *"
```

## GitHub Secrets

Add these repository secrets before enabling the scheduled workflow:

- `CLICKUP_API_TOKEN`
- `GOOGLE_SERVICE_ACCOUNT_JSON`
- `AUDIT_SHEET_URL`
- `PROJECT_NAME_FILTER` if you want to skip one or more exact project names
- `IGNORE_PDL_EMAILS` if you want to skip one or more Delivery Lead / PDL emails
- `HIGHLIGHT_TRACKER_ERRORS` if you want to override the default
- `RESOURCE_LOOKUP_TAB` if you use a non-default lookup tab later

## Notes

- The script already contains retry logic for Google Sheets read quota errors.
- Tracker header validation is strict against the first 20 columns (`A:T`).
- When no resource lookup is loaded, dashboard exports default the resource type to `Remote`.
