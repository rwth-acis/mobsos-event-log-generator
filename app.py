from flask import Flask, request, send_file
from sqlalchemy import create_engine
import pandas as pd
import os
import json
from event_log_generator.event_reader import get_db_connection
from event_log_generator.event_reader import generate_eventlog
import logging
import requests

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

# Define a logger
log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(level=logging.DEBUG, format=log_format, filename='app.log')
logger = logging.getLogger(__name__)



def generateXESfile(db_connection,start_date = None, end_date =None, resource_ids = None , include_bot_messages = False, include_life_cycle_start = False):
    logger.info('Reading events from database')
    event_log = generate_eventlog(db_connection, start_date, end_date, resource_ids, include_bot_messages=include_bot_messages, include_life_cycle_start=include_life_cycle_start)

    if event_log is None or event_log.empty:
        logger.info('No events found for resource ids: '+str(resource_ids))
        return None
    logger.info('Events read from database')

    if start_date is None:
        start_date = event_log['time:timestamp'].min().strftime('%Y-%m-%d')
    if end_date is None:
        end_date = event_log['time:timestamp'].max().strftime('%Y-%m-%d')

    file_name = 'event_log'+start_date+'_'+end_date+'.xes'
    logger.info('Writing event log to file: '+file_name)
    pm4py.write_xes(event_log, file_name, case_id_key='case:concept:name')
    return file_name


# route for generating event log for a specific resource
@app.route('/resource/<resource_id>', methods=['GET'])
def send_xml_file_for_resource(resource_id):
    print('Request received for resource', resource_id)
    if request.method == 'GET':
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        try:
            file_name = generateXESfile(db_connection,start_date, end_date, [resource_id])
            if file_name is None:
                return 'No events found for resource', 204
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
    logger.info('Request received for bot ' + botName)
    if request.method == 'GET':
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        include_bot_messages = request.args.get('include_bot_messages',False) 
        include_life_cycle_start = request.args.get('include_life_cycle_start',False) 
        if 'bot-manager-url' not in request.args:
            return {
                "error": "bot-manager-url parameter is missing"
            }, 400
        bot_manager_url = request.args.get('bot-manager-url')
        try:
            resource_ids = get_resource_ids_from_bot_manager(bot_manager_url,botName)
            # resource_ids +=  get_resource_ids_from_db(db_connection, botName)
            if len(resource_ids) == 0:
                return 'No resource ids found for bot', 500
            file_name = generateXESfile(db_connection,start_date, end_date, resource_ids, include_bot_messages=include_bot_messages, include_life_cycle_start=include_life_cycle_start)
            if file_name is None:
                return 'No events found for resource', 204
            return send_file(file_name, as_attachment=True), os.remove(file_name)
        except ValueError as e:
            print(e)
            logger.error(e)
            return str(e), 400
        except Exception as e:
            print(e)
            logger.error(e)
            return str(e), 500
    else:
        return 'Method not allowed', 405

if __name__ == '__main__':
    print('Starting event log generator')
    file_handler = logging.FileHandler('app.log')
    file_handler.setFormatter(logging.Formatter(log_format))
    logger.addHandler(file_handler)
    app.run(port=port, debug=True)


def get_resource_ids_from_bot_manager(bot_manager_url,botName):
    """
    This function returns the resource ids of the bot

    Parameters
    ----------
    bot_manager_url : string
        URL of the bot manager

    Returns
    -------
    resource_ids : list
        List of resource ids
    """
    if bot_manager_url is None:
        raise ValueError('bot_manager_url must be set')
    logger.info(f"Getting resource ids from bot manager {bot_manager_url}/bots")
    response = requests.get(bot_manager_url + '/bots')
    try:
        data = response.json()
        keys = []

        for key, value in data.items():
            if isinstance(value, dict) and "name" in value and value["name"] == botName:
                keys.append(key)
        if len(keys) == 0:
            logger.error(f"No resource ids found for bot {botName}")
        return keys

    except json.JSONDecodeError as e:
        print("Invalid JSON format:", e)
        return []

    
