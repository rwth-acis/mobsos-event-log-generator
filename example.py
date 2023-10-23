

from sqlalchemy import create_engine
import pandas as pd
import os
import pm4py
from event_log_generator import db_utils

from dotenv import load_dotenv
load_dotenv()

mysql_user = os.environ.get('JUPYTER_MYSQL_USER')
mysql_password = os.environ.get('JUPYTER_MYSQL_PASSWORD')
mysql_host = os.environ.get('JUPYTER_MYSQL_HOST')
mysql_db = os.environ.get('JUPYTER_MYSQL_DB')
mysql_port = os.environ.get('JUPYTER_MYSQL_PORT')

db_connection = db_utils.get_db_connection(
    mysql_host, mysql_port, mysql_user, mysql_password, mysql_db)
resource_ids = [os.environ.get('JUPYTER_RESOURCE_ID')]
event_log = db_utils.read_events_into_df(
    db_connection, None, None, resource_ids, None)
# remove SERVICE_CUSTOM_MESSAGE_2 events
event_log = event_log[event_log["EVENT_TYPE"] != "SERVICE_CUSTOM_MESSAGE_2"]
# remove lifecycle:transition = start events
event_log = event_log[event_log["lifecycle:transition"] != "start"]
event_log.head()

net , im , fm =pm4py.discover_petri_net_inductive(event_log)
net = pm4py.reduce_petri_net_invisibles(net)
net,im,fm = pm4py.reduce_petri_net_implicit_places(net, im, fm)
pm4py.view_petri_net(net,im, fm)


