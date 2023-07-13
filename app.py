from flask import Flask, request, send_file
from sqlalchemy import create_engine
import pandas as pd
import os
import pm4py

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
db_connection = create_engine(db_connection_str)

app = Flask(__name__)


def generateEventLog(start_date = None, end_date =None):
    if start_date is None or end_date is None:
        df = pd.read_sql('SELECT EVENT,CASE_ID,ACTIVITY_NAME, TIME_OF_EVENT, LIFECYCLE_PHASE, RESOURCE, RESOURCE_TYPE, REMARKS FROM LAS2PEERMON.MESSAGE WHERE CASE_ID IS NOT NULL', con=db_connection)
    else:
        statement = 'SELECT EVENT,CASE_ID,ACTIVITY_NAME, TIME_OF_EVENT, LIFECYCLE_PHASE, RESOURCE, RESOURCE_TYPE, REMARKS FROM LAS2PEERMON.MESSAGE WHERE CASE_ID IS NOT NULL AND TIME_STAMP BETWEEN %s AND %s'
        # format the statement
        df = pd.read_sql(statement, con=db_connection, params=(start_date, end_date))
    # rename columns CASE_ID->case:concept:name, ACTIVITY_NAME->concept:name, TIME_OF_EVENT->time:timestamp, LIFECYCLE_PHASE->lifecycle:transition
    df.rename(columns={'CASE_ID': 'case:concept:name', 'ACTIVITY_NAME': 'concept:name', 'TIME_OF_EVENT': 'time:timestamp', 'LIFECYCLE_PHASE': 'lifecycle:transition'}, inplace=True)
    file_name = 'event_log'+start_date+'_'+end_date+'.xes'
    pm4py.write_xes(df, file_name, case_id_key='case:concept:name')
    return file_name

@app.route('/', methods=['GET'])
def send_xml_file():
    print('Request received')
    if request.method == 'GET':
        # Get the start and end date from the request
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        # make sure that no sql injection is possible by using 

        # check if the dates are valid
        if start_date is None or end_date is None:
            return 'Invalid dates', 400
        
        file_name = generateEventLog(start_date, end_date)

        return send_file(file_name, as_attachment=True), os.remove(file_name)

if __name__ == '__main__':
    
    app.run(port=port, debug=True)
