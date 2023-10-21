import sqlalchemy
import pandas as pd

# pip install dogpile.cache for caching the sql results

def read_events_into_df(db_connection,start_date = None, end_date =None, resource_ids = None, botName = None):
    """
    This function reads the events from the database and returns a pandas dataframe
    """
    if db_connection is None:
        raise ValueError('db_connection must be set')
    print(f'Reading events from database from {start_date} until {end_date}')
    if resource_ids is None:
        resource_ids = get_resource_ids(db_connection, botName)
    if start_date is None or end_date is None:
        statement = 'SELECT EVENT_TYPE,CASE_ID,ACTIVITY_NAME, TIME_STAMP, LIFECYCLE_PHASE, RESOURCE, RESOURCE_TYPE, REMARKS FROM LAS2PEERMON.EVENTLOG WHERE CASE_ID IS NOT NULL AND RESOURCE IN %s'
        df = pd.read_sql(statement, con=db_connection, params=(resource_ids,))
    else:
        statement = 'SELECT EVENT_TYPE,CASE_ID,ACTIVITY_NAME, TIME_STAMP, LIFECYCLE_PHASE, RESOURCE, RESOURCE_TYPE, REMARKS FROM LAS2PEERMON.EVENTLOG WHERE CASE_ID IS NOT NULL AND RESOURCE IN %s AND TIME_STAMP BETWEEN %s AND %s'
        # format the statement
        df = pd.read_sql(statement, con=db_connection, params=(resource_ids,start_date, end_date))
    df.rename(columns={'CASE_ID': 'case:concept:name', 'ACTIVITY_NAME': 'concept:name', 'TIME_STAMP': 'time:timestamp', 'LIFECYCLE_PHASE': 'lifecycle:transition'}, inplace=True)
    # convert time:timestamp to datetime
    df['time:timestamp'] = pd.to_datetime(df['time:timestamp'])
    return df

def get_db_connection(host,port, user, password, db = 'LAS2PEERMON'):
    if(host is None or user is None or password is None):
        raise ValueError('mysql host, user and password must be set')
    db_connection = sqlalchemy.create_engine(f'mysql+pymysql://{user}:{password}@{host}:{port}/{db}')
    #test connection
    db_connection.connect()
    if db_connection is None:
        raise ValueError('Could not connect to database')
    return db_connection

def get_resource_ids(db_connection,botName):
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
    df = pd.read_sql('SELECT REMARKS->>"$.agentId" as id from MESSAGE where REMARKS->>"$.botName"=%s', con=db_connection, params=(botName,))

    return list(filter(lambda value: value is not None,df['id'].values.tolist()))
