from dotenv import load_dotenv
from flask import Flask, request, send_file
from sqlalchemy import create_engine
import pandas as pd
import os
import json
from event_log_generator.event_reader import get_db_connection
from event_log_generator.event_reader import generate_eventlog
import logging
import requests
from flask_apscheduler import APScheduler
import shutil
from tasks import clean_directory

try:
    import psutil

    parent_pid = os.getpid()
    parent_name = str(psutil.Process(parent_pid).name())
except psutil.NoSuchProcess:
    print("No such process")
    parent_name = "unknown"
import pm4py


# current directory
current_dir = os.path.dirname(os.path.realpath(__file__))
# load environment variables
load_dotenv(dotenv_path=current_dir+'/.env')

mysql_user = os.environ['MYSQL_USER']
mysql_password = os.environ['MYSQL_PASSWORD']
mysql_host = os.environ['MYSQL_HOST']
mysql_db = os.environ['MYSQL_DB']
mysql_port = os.environ['MYSQL_PORT']

cleanup_interval = os.environ.get('CLEANUP_INTERVAL', 60)

db_connection = get_db_connection(
    mysql_host, mysql_port, mysql_user, mysql_password, mysql_db)

# port
port = os.environ['PORT']
# set port to 8086 if not set
if port is None:
    port = 8087


class Config(object):
    JOBS = [
        {
            'id': 'cleaning_job',
            'func': 'tasks:clean_directory',
            'args': (os.path.join(current_dir, 'event_logs'),),
            'trigger': 'interval',
            'seconds': int(cleanup_interval)
        }
    ]

    SCHEDULER_API_ENABLED = True


app = Flask(__name__)
app.config.from_object(Config())

# Define a logger
log_format = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(level=logging.DEBUG, format=log_format, filename='app.log')
logger = logging.getLogger(__name__)

scheduler = APScheduler()
scheduler.init_app(app)
scheduler.start()


@app.route('/resource/<resource_id>', methods=['GET'])
def send_xml_file_for_resource(resource_id):
    logger.info('Request received for resource ' + resource_id)
    start_date = request.args.get('start_date', None)
    end_date = request.args.get('end_date', None)
    include_bot_messages = request.args.get('include_bot_messages', False)
    include_life_cycle_start = request.args.get(
        'include_life_cycle_start', False)
    use_cache = request.args.get('use_cache', False)
    file_name = get_filename(start_date, end_date, [resource_id],
                             include_bot_messages, include_life_cycle_start)
    if use_cache is not None and os.path.exists(os.path.join(current_dir, 'event_logs', file_name)):
        return send_file(os.path.join(current_dir, 'event_logs', file_name), as_attachment=True)
    else:
        try:
            file_name = generateXESfile(
                db_connection, start_date, end_date, [resource_id], include_bot_messages, include_life_cycle_start)
            if file_name is None:
                return 'No events found for resource', 204
            return send_file(os.path.join(current_dir, 'event_logs', file_name), as_attachment=True)
        except ValueError as e:
            return str(e), 400
        except Exception as e:
            return str(e), 500


@app.route('/resources', methods=['POST'])
def send_xml_files_for_resources():
    body = request.get_json()
    resource_ids = body.get('resource_ids', [])
    start_date = request.args.get('start_date', None)
    end_date = request.args.get('end_date', None)
    include_bot_messages = request.args.get('include_bot_messages', False)
    include_life_cycle_start = request.args.get(
        'include_life_cycle_start', False)
    use_cache = request.args.get('use_cache', False)

    file_name = get_filename(start_date, end_date, resource_ids,
                             include_bot_messages, include_life_cycle_start)
    if use_cache is not None and os.path.exists(os.path.join(current_dir, 'event_logs', file_name)):
        return send_file(os.path.join(current_dir, 'event_logs', file_name), as_attachment=True)
    else:
        try:
            file_name = generateXESfile(
                db_connection, start_date, end_date, resource_ids, include_bot_messages, include_life_cycle_start)
            if file_name is None:
                return 'No events found for resource', 204
            return send_file(os.path.join(current_dir, 'event_logs', file_name), as_attachment=True)
        except ValueError as e:
            return str(e), 400
        except Exception as e:
            return str(e), 500


@app.route('/bot/<botName>', methods=['GET'])
def send_xml_file_for_bot(botName):
    logger.info('Request received for bot ' + botName)
    if request.method == 'GET':
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        include_bot_messages = request.args.get('include_bot_messages', False)
        include_life_cycle_start = request.args.get(
            'include_life_cycle_start', False)
        use_cache = request.args.get('use_cache', False)
        if 'bot-manager-url' not in request.args:
            return {
                "error": "bot-manager-url parameter is missing"
            }, 400
        bot_manager_url = request.args.get('bot-manager-url')
        try:
            resource_ids = get_resource_ids_from_bot_manager(
                bot_manager_url, botName)

            if len(resource_ids) == 0:
                return 'No resource ids found for bot', 500
            file_name = get_filename(start_date, end_date, resource_ids,
                                     include_bot_messages, include_life_cycle_start)
            if use_cache and os.path.exists(os.path.join(current_dir, file_name)):
                return send_file(file_name, as_attachment=True)
            else:
                file_name = generateXESfile(db_connection, start_date, end_date, resource_ids,
                                            include_bot_messages=include_bot_messages, include_life_cycle_start=include_life_cycle_start)
                if file_name is None:
                    return 'No events found for resource', 204
                return send_file(file_name, as_attachment=True)
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
    file_handler = logging.FileHandler('app.log')
    file_handler.setFormatter(logging.Formatter(log_format))
    logger.addHandler(file_handler)
    app.run(port=port, debug=True)


def generateXESfile(db_connection, start_date=None, end_date=None, resource_ids=None, include_bot_messages=False, include_life_cycle_start=False):
    logger.info('Reading events from database')
    if not os.path.exists(os.path.join(current_dir, 'event_logs')):
        os.makedirs(os.path.join(current_dir, 'event_logs'))
    event_log = generate_eventlog(db_connection, start_date, end_date, resource_ids,
                                  include_bot_messages=include_bot_messages, include_life_cycle_start=include_life_cycle_start)

    if event_log is None or event_log.empty:
        logger.info('No events found for resource ids: '+str(resource_ids))
        return None
    logger.info('Events read from database')

    file_name = get_filename(start_date, end_date, resource_ids,
                             include_bot_messages, include_life_cycle_start)
    pm4py.write_xes(event_log, 'event_logs/'+file_name,
                    case_id_key='case:concept:name')
    return file_name


def get_filename(start_date=None, end_date=None, resource_ids=None, include_bot_messages=False, include_life_cycle_start=False):
    filename = f"{''.join(resource_ids)}"
    if start_date is not None:
        filename += f"-{start_date}"
    if end_date is not None:
        filename += f"_{end_date}"
    if include_bot_messages:
        filename += "_bot_messages"
    if include_life_cycle_start:
        filename += "_life_cycle_start"
    return filename + '.xes'


def get_resource_ids_from_bot_manager(bot_manager_url, botName):
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
    logger.info(
        f"Getting resource ids from bot manager {bot_manager_url}/bots")
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
