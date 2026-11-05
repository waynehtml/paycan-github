# Databricks notebook source
import json
import requests
import datetime
import hashlib
import hmac
import base64
from enum import Enum, auto

# COMMAND ----------

# Secrets from the Key Vault
workspace_id = dbutils.secrets.get(scope = "lake-kv-paycan-scope", key = "log-platform-logspaceid")
shared_key = dbutils.secrets.get(scope = "lake-kv-paycan-scope", key = "log-platform-appkey")
   
# Hardcoded table name in Custom Logs of DataLake Log Analytics Workspace
default_log_table_name = "PayCan_Databricks_General"

# COMMAND ----------

# Calculates information regarding the notebook the logging utility was called from as well as run information
notebook_name = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
notebook_data = json.loads(dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson())
  
# Build the API signature
def build_signature(date, content_length, method, content_type, resource):
    x_headers = 'x-ms-date:' + date
    string_to_hash = method + "\n" + str(content_length) + "\n" + content_type + "\n" + x_headers + "\n" + resource
    bytes_to_hash = bytes(string_to_hash, encoding="utf-8")
    decoded_key = base64.b64decode(shared_key)
    encoded_hash = base64.b64encode(hmac.new(decoded_key, bytes_to_hash, digestmod=hashlib.sha256).digest()).decode()
    authorization = "SharedKey {}:{}".format(workspace_id,encoded_hash)
    return authorization
  
# Send the log to the default table as indicated in the hardcoded name
def send_log(level, message, log_table_name):
    method = 'POST'
    content_type = 'application/json'
    resource = '/api/logs'
    rfc1123date = datetime.datetime.utcnow().strftime('%a, %d %b %Y %H:%M:%S GMT')
    log_json_object = {'Level': level, 'Message': str(message)}
    log_json_object.update({'notebook-name': notebook_name})
    log_json_object.update({'notebook-data': notebook_data})
    body = json.dumps(log_json_object)
    content_length = len(body)
    signature = build_signature(rfc1123date, content_length, method, content_type, resource)
    uri = 'https://' + workspace_id + '.ods.opinsights.azure.com' + resource + '?api-version=2016-04-01'

    headers = {
        'content-type': content_type,
        'Authorization': signature,
        'Log-Type': log_table_name,
        'x-ms-date': rfc1123date
    }

    response = requests.post(uri,data=body, headers=headers)
    if (response.status_code >= 200 and response.status_code <= 299):
        print('Accepted - Log Item Has Been Sent To  The Log Analytics Workspace')
        # print(body)
    else:
        print("Response Code: {}".format(response.status_code))

# Sends a log with a critical level to Log Analytics
def log_critical(message, log_table_name = default_log_table_name):
  send_log('CRITICAL', message, log_table_name)

# Sends a log with an error level to Log Analytics
def log_error(message, log_table_name = default_log_table_name):
  send_log('ERROR', message, log_table_name)

# Sends a log with a warning level to Log Analytics
def log_warning(message, log_table_name = default_log_table_name):
  send_log('WARNING', message, log_table_name)

# Sends a log with an info level to Log Analytics
def log_info(message, log_table_name = default_log_table_name):
  send_log('INFO', message, log_table_name)

# Sends a log with a debug level to Log Analytics
def log_debug(message, log_table_name = default_log_table_name):
  send_log('DEBUG', message, log_table_name)
