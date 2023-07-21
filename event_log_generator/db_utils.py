import sqlalchemy
import pandas as pd

# pip install dogpile.cache for caching the sql results
"""
This function reads the events from the database and returns a pandas dataframe
"""
def read_events_into_df(db_connection,start_date = None, end_date =None):
    print('Reading events from database', start_date, end_date)
    if start_date is None or end_date is None:
        df = pd.read_sql('SELECT EVENT,CASE_ID,ACTIVITY_NAME, TIME_OF_EVENT, LIFECYCLE_PHASE, RESOURCE, RESOURCE_TYPE, REMARKS FROM LAS2PEERMON.MESSAGE WHERE CASE_ID IS NOT NULL', con=db_connection)
    else:
        statement = 'SELECT EVENT,CASE_ID,ACTIVITY_NAME, TIME_OF_EVENT, LIFECYCLE_PHASE, RESOURCE, RESOURCE_TYPE, REMARKS FROM LAS2PEERMON.MESSAGE WHERE CASE_ID IS NOT NULL AND TIME_STAMP BETWEEN %s AND %s'
        # format the statement
        df = pd.read_sql(statement, con=db_connection, params=(start_date, end_date))
    # rename columns CASE_ID->case:concept:name, ACTIVITY_NAME->concept:name, TIME_OF_EVENT->time:timestamp, LIFECYCLE_PHASE->lifecycle:transition
    df.rename(columns={'CASE_ID': 'case:concept:name', 'ACTIVITY_NAME': 'concept:name', 'TIME_OF_EVENT': 'time:timestamp', 'LIFECYCLE_PHASE': 'lifecycle:transition'}, inplace=True)
    return df