import requests
import json
import os
import pandas as pd
from datetime import datetime as dt
from dotenv import load_dotenv
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import io

load_dotenv("../.env")

SF_ACCOUNT    = os.getenv('SF_ACCOUNT')
SF_USER       = os.getenv('SF_USER')
SF_WAREHOUSE  = os.getenv('SF_WAREHOUSE')
SF_DATABASE  = os.getenv('SF_DATABASE')
SF_ROLE     = os.getenv('SF_ROLE')
SF_SCHEMA     = os.getenv('SF_SCHEMA')
SF_PASSWORD   = os.getenv('SF_PASSWORD')

# conn_string = f"snowflake://{SF_USER}:{SF_PASSWORD}@{SF_ACCOUNT}/{SF_WAREHOUSE}/{SF_SCHEMA}"
# engine = create_engine(conn_string)



#fire up an instance of a snowflake connection
connection = snowflake.connector.connect (
account  = SF_ACCOUNT,
user     = SF_USER,
password = SF_PASSWORD,
database = SF_DATABASE,
warehouse = SF_WAREHOUSE,
schema = SF_SCHEMA,
role = SF_ROLE
)
con = connection.cursor()



#print(os.getenv('email_id'))

# read data from .env file
email_id=os.getenv('email_id')
email_pass=os.getenv('email_pass')
header_authoriation=os.getenv('header_authoriation')
api_instance=os.getenv('api_instance')
client_secret=os.getenv('client_secret')
client_id=os.getenv('client_id')

### Global variables ###

#outputfile_ts = str(dt.now().strftime("%Y%m%d_%H%M%S")) # used to add timestamp into output file name

outputfile_ts = str(dt.now().strftime("%Y%m%d")) # used to add timestamp into output file name


##### Step 1 : Read Token from Token API  #####


url = "https://EU.api.blackline.com/connect/token"

payload="grant_type=password&username="+email_id+"%40fortum.com&password="+email_pass+"&scope=ReportsAPI%20"+api_instance+"&client_secret="+client_secret+"&client_id="+client_id
#print(payload)

authorization =  "Basic "+header_authoriation

#'Authorization': 'Basic asdfasfjalsdfas',
headers = {
  'Authorization': authorization,
  'Content-Type': 'application/x-www-form-urlencoded'
}

response = requests.request("POST", url, headers=headers, data=payload)

#print(response.text)


vtoken = json.loads(response.text) # convers string into dictionary

#print(vtoken.keys())

#print(vtoken["access_token"])

#print(vtoken["token_type"])


##### Step 2 : Read Request ID from query runs API using by using access token #####

url = "https://EU.api.blackline.com/api/queryruns"


payload={}
headers = {
  #'Authorization': 'Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJSUzI1NiIsIng1dCI6ImZzQlJ3X21zUzNaSmQ5cEt5NmxiVldwS1M4QSIsImtpZCI6ImZzQlJ3X21zUzNaSmQ5cEt5NmxiVldwS1M4QSJ9.eyJleHAiOjE2NzQ4MTczMjcsImlzcyI6Imh0dHBzOi8vZXUuYXBpLmJsYWNrbGluZS5jb20iLCJhdWQiOiJodHRwczovL2V1LmFwaS5ibGFja2xpbmUuY29tL3Jlc291cmNlcyIsImNsaWVudF9pZCI6ImZvcnR1bV9TVFMyIiwic2NvcGUiOlsiaW5zdGFuY2VfMDhDRTIyNzYtRUE2MS00RTc5LUFBRTUtRjY4NUMxM0I0QkUxIiwiUmVwb3J0c0FQSSJdLCJzdWIiOiIzMTQwNjc4Qy1EOTk2LTRBRjAtOTVDNi1ENDRGREEzMzJBOEEiLCJhdXRoX3RpbWUiOjE2NzQ4MTM3MjcsImlkcCI6Imlkc3J2In0.GtfAMrYi3gy_PWB8JvpTvk6y_ci4n71K3LZs96lpORDMUd_aOhLamFm_0ASb_PXpClbLaffJnhtbWJFJadFrosjrYJr8ubNUxozmcW9DbXaB7xbTi4s732aVmDG3yY-3B-DGUqAqtWwmmZsAmNcT4KKNfqXEMWIuMMJHslejx0opKYmIFs8UzVpDh-TS88DJ-sGXmpcDku_q8YvFz6JooABk3Q7JRuYV6pGytxTeElwLzV0NtmwZJVTr_GrM6PNH0Z-1Tvqw-4rNz2C-nGd3mnepLDyYSy2nXBKomFIXxMMqxOq6uxDk8oecoqTkiub2jAjj0KUuAecMWkq80-mdGWh2nG4cuGLFRj0uV-UltTYajjIy1QTN7j6yQpgOYxJpRRAHUat03mWhM_8yQmCh3bGFvjyPtcIPK3IKeFefS19qTjDZa3yq8unNSdVOFzeQxi630sl4URYtwE6NMrQuiCingdEW30ULyM2RDbDyVNJRuznr--nE4G1EQkvF7OKP4W1j5tNitIfbjvlfkAoXG0c6vTanSUnYDA4l6Xar6OdaTBejVd7uQzsKedX1xxrgzcJRUoBZ679CQidDl-EuvO5UyFeCUPEsdVpwnWFF3gDWf4dbr4RymqUw6625cXcApbrBoM7clLbgV6oYQJ-3iBJrjVPVan4cA52vHBdemDs'
  'Authorization': 'Bearer ' + vtoken["access_token"]
}

getresponse = requests.request("GET", url, headers=headers, data=payload)

#print(getresponse.text)


vrequest_data = json.loads(getresponse.text) # convers string into dictionary

#print(vrequest_data)

df = pd.DataFrame(vrequest_data)
# print (df["id"]) # Read requestID
# print (df["name"]) # Read requestID
# print (df["startTime"]) # Read requestID


df= df[(df['name'] == 'Task Management data - for QlikSense reporting') & (df['status'] == 'Complete')]


df['startTime'] = pd.to_datetime(df['startTime'], errors='coerce',format='%Y-%m-%d').dt.tz_convert(tz='EET') + pd.Timedelta(hours=8)

#print (df["startTime"]) # Read requestID

vrequestid = str(df["id"].values[0])

#print(f' print request id {vrequestid}')

##### Step 3 : Read Actual report Data #####


url = "https://EU.api.blackline.com/api/completedqueryrun/"+vrequestid+"/CSV"

#print(url)


payload={}
headers = {
  'Authorization': 'Bearer ' + vtoken["access_token"]
}

response = requests.request("GET", url, headers=headers, data=payload)

#print(response.text)

# open the file in the write mode

# try:
#   file = open("Blackline_Task_Report_" +vrequestid+'_'+outputfile_ts+ '.csv','w',encoding='utf-8')
#   #writer = csv.writer(file)
#   file.write(response.text) 
#   file.close()

# except:
#   print("Unable to create outfile")


#vtask_data = json.loads(response.text) # converts string into dictionary

#print(vrequest_data)

df_task = pd.read_csv(io.StringIO(response.text), sep=",")

#print(df_task.dtypes)



#print(df_task["Period End Date"].head(5))
# df_task['Task Due Date'] = pd.to_datetime(df_task['Task Due Date']).dt.strftime("%m/%d/%Y %H:%M:%S")
# df_task['Period End Date'] = pd.to_datetime(df_task['Task Due Date']).dt.strftime("%m/%d/%Y %H:%M:%S")
# df_task['Prepared Date'] = pd.to_datetime(df_task['Prepared Date']).dt.strftime("%m/%d/%Y %H:%M:%S")


#What to do if the table exists? replace, append, or fail?
table_name = "DWF_BL_TASKS_DATA"
if_exists = 'replace'

#Write the data to Snowflake, using pd_writer to speed up loading

try:
  con.execute(    
    "TRUNCATE TABLE DWF_BL_TASKS_DATA"   ) 
  success, nchunks, nrows, _ = write_pandas(connection, df_task, table_name, auto_create_table=True)
  print("Data loaded into snowflake table DWF_BL_TASKS_DATA")  
  
except Exception as e:  
    raise e
finally:  
  con.close() 
connection.close()
