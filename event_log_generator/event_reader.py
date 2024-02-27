import sqlalchemy
import pandas as pd
import json
# pip install dogpile.cache for caching the sql results


def generate_eventlog(db_connection, start_date=None, end_date=None, resource_ids=None, include_bot_messages=False, include_life_cycle_start=False, deserialize_remarks=False):
    df = _read_events_into_df(db_connection, start_date, end_date, resource_ids,
                              include_bot_messages=include_bot_messages, include_life_cycle_start=include_life_cycle_start)

    df['EVENT_TYPE'] = df['EVENT_TYPE'].replace(
        'SERVICE_CUSTOM_MESSAGE_1', 'USER_MESSAGE')
    df['EVENT_TYPE'] = df['EVENT_TYPE'].replace(
        'SERVICE_CUSTOM_MESSAGE_2', 'BOT_MESSAGE')
    df['EVENT_TYPE'] = df['EVENT_TYPE'].replace(
        'SERVICE_CUSTOM_MESSAGE_3', 'SERVICE_REQUEST')

    if not include_bot_messages:
        df = df[(df['EVENT_TYPE'] == 'SERVICE_REQUEST')
                | (df['EVENT_TYPE'] == 'USER_MESSAGE')]
    if not include_life_cycle_start:
        df = df[(df['lifecycle:transition'] == 'complete')]

    if df.empty:
        return df
    if start_date is None:
        start_date = df['time:timestamp'].min().strftime('%Y-%m-%d')
    if end_date is None:
        end_date = df['time:timestamp'].max().strftime('%Y-%m-%d')

    # extract fields from remarks column
    if deserialize_remarks:
        df = df.apply(_extract_remarks, axis=1)
    if ('lifecycle:transition' in df.columns):
        df.loc[:, ['lifecycle:transition']] = df[[
            'lifecycle:transition']].fillna('complete')
    if ("serviceEndpoint" in df.columns):
        df.loc[:, ["serviceEndpoint"]] = df[["serviceEndpoint"]].fillna('')
    if ("user" in df.columns):
        df.loc[:, ["user"]] = df[["user"]].fillna('')
    if ("in-service-context" in df.columns):
        df.loc[:, ["in-service-context"]
               ] = df[["in-service-context"]].fillna(False)
    df['time:timestamp'] = pd.to_datetime(df['time:timestamp'])
    return df


def _read_events_into_df(db_connection, start_date=None, end_date=None, resource_ids=None, botName=None, include_bot_messages=False, include_life_cycle_start=False):
    """
    This function reads the events from the database and returns a pandas dataframe
    """
    params = tuple()
    if db_connection is None:
        raise ValueError('db_connection must be set')
    print(f'Reading events from database from {start_date} until {end_date}')
    if resource_ids is None:
        resource_ids = get_resource_ids_from_db(db_connection, botName)
    params = (resource_ids,)
    statement = 'SELECT EVENT_TYPE,CASE_ID,ACTIVITY_NAME, TIME_STAMP, LIFECYCLE_PHASE, RESOURCE, RESOURCE_TYPE, REMARKS FROM LAS2PEERMON.EVENTLOG WHERE CASE_ID IS NOT NULL AND RESOURCE IN %s'
    if not include_bot_messages:
        statement += 'AND EVENT_TYPE != "SERVICE_CUSTOM_MESSAGE_2"'
    if not include_life_cycle_start:
        statement += 'AND LIFECYCLE_PHASE != "start"'
    if start_date is not None:
        statement += 'AND TIME_STAMP >= %s'
        params += (start_date,)
    if end_date is not None:
        statement += 'AND TIME_STAMP <= %s'
        params += (end_date,)

    df = pd.read_sql(statement, con=db_connection,
                     params=params)
    print(f'Read {len(df)} events from database')
    df.rename(columns={'CASE_ID': 'case:concept:name', 'ACTIVITY_NAME': 'concept:name',
              'TIME_STAMP': 'time:timestamp', 'LIFECYCLE_PHASE': 'lifecycle:transition'}, inplace=True)
    # convert time:timestamp to datetime
    df['time:timestamp'] = pd.to_datetime(df['time:timestamp'])
    return df


def get_db_connection(host, port, user, password, db='LAS2PEERMON'):
    if (host is None or user is None or password is None):
        raise ValueError('mysql host, user and password must be set')
    db_connection = sqlalchemy.create_engine(
        f'mysql+pymysql://{user}:{password}@{host}:{port}/{db}')
    # test connection
    db_connection.connect()
    if db_connection is None:
        raise ValueError('Could not connect to database')
    return db_connection


def get_resource_ids_from_db(db_connection, botName):
    """
    This function returns the resource ids of the bot

    Parameters
    ----------
    db_connection : sqlalchemy connection
        Connection to the database
    botName : string
        Name of the bot

    Returns
    -------
    resource_ids : list
        List of resource ids
    """
    if db_connection is None:
        raise ValueError('db_connection must be set')
    if botName is None:
        raise ValueError('botName must be set')
    df = pd.read_sql('SELECT REMARKS->>"$.agentId" as id from LAS2PEERMON.MESSAGE where REMARKS->>"$.botName"=%s',
                     con=db_connection, params=(botName,))

    return list(filter(lambda value: value is not None, df['id'].values.tolist()))


def _extract_remarks(row):
    """
    Extracts the fields from the remarks column and adds them to the row
    """
    json_data = json.loads(row['REMARKS'])
    for key in json_data.keys():
        row[key] = json_data[key]
    return row
