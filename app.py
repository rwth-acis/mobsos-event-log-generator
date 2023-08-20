from flask import Flask, request, send_file
from sqlalchemy import create_engine
import pandas as pd
import os

from event_log_generator import read_events_into_df
from event_log_generator import get_db_connection
from event_log_generator import get_resource_ids

try:
    import psutil

    parent_pid = os.getpid()
    parent_name = str(psutil.Process(parent_pid).name())
except psutil.NoSuchProcess:
    print("No such process")
    parent_name = "unknown"
import pm4py



# pip install dogpile.cache for caching the sql results

# current directory
current_dir = os.path.dirname(os.path.realpath(__file__))
# load environment variables
from dotenv import load_dotenv
load_dotenv(dotenv_path=current_dir+'/.env')

mysql_user = os.environ['MYSQL_USER']
mysql_password = os.environ['MYSQL_PASSWORD']
mysql_host = os.environ['MYSQL_HOST']
mysql_db = os.environ['MYSQL_DB']
mysql_port = os.environ['MYSQL_PORT']

db_connection = get_db_connection(mysql_host,mysql_port, mysql_user, mysql_password, mysql_db)

# port 
port = os.environ['PORT']
#set port to 8086 if not set
if port is None:
    port = 8087


app = Flask(__name__)


def generateEventLog(db_connection,start_date = None, end_date =None, resource_ids = None):
    print('Reading events from database', start_date, end_date)
    df = read_events_into_df(db_connection,start_date, end_date,resource_ids)
    # rename columns CASE_ID->case:concept:name, ACTIVITY_NAME->concept:name, TIME_OF_EVENT->time:timestamp, LIFECYCLE_PHASE->lifecycle:transition
    df.rename(columns={'CASE_ID': 'case:concept:name', 'ACTIVITY_NAME': 'concept:name', 'TIME_OF_EVENT': 'time:timestamp', 'LIFECYCLE_PHASE': 'lifecycle:transition'}, inplace=True)

    df['EVENT'] = df['EVENT'].replace('SERVICE_CUSTOM_MESSAGE_1', 'USER_MESSAGE')
    df['EVENT'] = df['EVENT'].replace('SERVICE_CUSTOM_MESSAGE_2', 'BOT_MESSAGE')
    df['EVENT'] = df['EVENT'].replace('SERVICE_CUSTOM_MESSAGE_3', 'SERVICE_REQUEST')

    df['time:timestamp'] = pd.to_datetime(df['time:timestamp'])
    if df.empty:
        raise ValueError('No events found in database')
    if start_date is None:
        start_date = df['time:timestamp'].min().strftime('%Y-%m-%d')
    if end_date is None:
        end_date = df['time:timestamp'].max().strftime('%Y-%m-%d')
    file_name = 'event_log'+start_date+'_'+end_date+'.xes'
    pm4py.write_xes(df, file_name, case_id_key='case:concept:name')
    return file_name


# route for generating event log for a specific resource
@app.route('/resource/<resource_id>', methods=['GET'])
def send_xml_file_for_resource(resource_id):
    print('Request received for resource', resource_id)
    if request.method == 'GET':
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        try:
            file_name = generateEventLog(db_connection,start_date, end_date, list(resource_id))
            return send_file(file_name, as_attachment=True), os.remove(file_name)
        except ValueError as e:
            return str(e), 400
        except Exception as e:
            return str(e), 500
    else:
        return 'Method not allowed', 405

# route for generating event log for a bot name
@app.route('/bot/<botName>', methods=['GET'])
def send_xml_file_for_bot(botName):
    print('Request received for bot', botName)
    if request.method == 'GET':
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        try:
            resource_ids = get_resource_ids(db_connection, botName)
            file_name = generateEventLog(db_connection,start_date, end_date, resource_ids)
            return send_file(file_name, as_attachment=True), os.remove(file_name)
        except ValueError as e:
            return str(e), 400
        except Exception as e:
            return str(e), 500
    else:
        return 'Method not allowed', 405

if __name__ == '__main__':
    print('Starting event log generator')
    app.run(port=port, debug=True)
