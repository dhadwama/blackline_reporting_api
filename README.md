# Blackline_Reporting_API
Blackline Reporting API

get_blackline_api_tasks.py
A Python script to interact with BlackLine’s Reporting and Tasks API—specifically designed to fetch task data from your BlackLine environment using OAuth 2.0 authentication.

🔧 Features
OAuth 2.0 Authentication: Acquires access tokens via the BlackLine Security Token Service (STS).

Report Listing: Retrieves the list of report runs (Report List API).

Report Downloading: Fetches completed report data in formats like CSV, Excel, or PDF (Reporting API).

Task Status Polling: Submits tasks for import and polls their status via the Tasks API endpoint.

🧩 Prerequisites
Python 3.7+

Dependencies (install via pip):
pip install requests python-dotenv

BlackLine API access:

Client ID and Client Secret

API user assigned to the Tasks and/or Reporting scope

Your BlackLine instance's STS token endpoint (e.g. https://<region>.api.blackline.com/authorize/connect/token) and base API endpoints


🛠️ Configuration
Create a .env file in the same directory:

BLACKLINE_CLIENT_ID=your_client_id
BLACKLINE_CLIENT_SECRET=your_client_secret
BLACKLINE_USERNAME=your_api_username
BLACKLINE_PASSWORD=your_api_key
BLACKLINE_REGION=us2  # e.g., 'us2' or 'eu1'

This configures the authentication flow and API region.

python get_blackline_api_tasks.py

This will:

Authenticate via OAuth 2.0 to receive an access token

List available report runs via /api/queryruns

Download completed report data via /api/completedqueryrun/{id}/{format} 
scribd.com

Submit a batch of tasks via /tasks/import and poll status at /tasks/import/status/{jobid} 

📝 Example Output
Access token obtained, expires in 3600 seconds
Found 5 report runs:
- ID 66886: Completed ✅
Fetching CSV for report 66886...
Saved to report_66886.csv
Submitting 100 tasks...
Job ID: abc123 — status: Queued → Running → Finished


👷 Extending & Customizing
Add new CLI arguments (e.g., report ID, file format).

Integrate with your ETL pipeline or scheduler.

Implement asynchronous polling or retries for task submission.

Secure credentials via Azure Key Vault, AWS SSM, etc., instead of .env.

📚 References
BlackLine OAuth & Reporting API: token endpoint, /api/queryruns, /api/completedqueryrun 
scribd.com

Tasks API: /tasks/import, /tasks/import/status, JSON payload structure 
scribd.com
