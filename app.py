from flask import Flask, request, send_file
from sqlalchemy import create_engine
import pandas as pd
import os
import pm4py
from event_log_generator.db_utils import read_events_into_df

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

# port 
port = os.environ['PORT']
#set port to 8086 if not set
if port is None:
    port = 8087

db_connection_str = f'mysql+pymysql://{mysql_user}:{mysql_password}@{mysql_host}/{mysql_db}'


app = Flask(__name__)


def generateEventLog(db_connection,start_date = None, end_date =None):
    df = read_events_into_df(db_connection,start_date, end_date)
    # rename columns CASE_ID->case:concept:name, ACTIVITY_NAME->concept:name, TIME_OF_EVENT->time:timestamp, LIFECYCLE_PHASE->lifecycle:transition
    df.rename(columns={'CASE_ID': 'case:concept:name', 'ACTIVITY_NAME': 'concept:name', 'TIME_OF_EVENT': 'time:timestamp', 'LIFECYCLE_PHASE': 'lifecycle:transition'}, inplace=True)
    df['time:timestamp'] = pd.to_datetime(df['time:timestamp'])
    if start_date is None:
        start_date = df['time:timestamp'].min().strftime('%Y-%m-%d')
    if end_date is None:
        end_date = df['time:timestamp'].max().strftime('%Y-%m-%d')
    file_name = 'event_log'+start_date+'_'+end_date+'.xes'
    pm4py.write_xes(df, file_name, case_id_key='case:concept:name')
    return file_name

@app.route('/', methods=['GET'])
def send_xml_file():
    print('Request received')
    if request.method == 'GET':
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')

        # if start_date is None: 
        #     start_date = '2021-01-01'
        
        # if end_date is None:
        #     end_date = pd.to_datetime('now').strftime('%Y-%m-%d')
        
        file_name = generateEventLog(db_connection,start_date, end_date)

        return send_file(file_name, as_attachment=True), os.remove(file_name)

if __name__ == '__main__':
    db_connection = create_engine(db_connection_str)
    app.run(port=port, debug=True)
