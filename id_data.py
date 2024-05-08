import requests
import pandas as pd
import yaml
import json
import os
from datetime import datetime, timezone, timedelta
from application_logging.logger import logger
import itertools
import gspread


# Params
params_path = 'params.yaml'


def read_params(config_path):
    with open(config_path) as yaml_file:
        config = yaml.safe_load(yaml_file)
    return config


config = read_params(params_path)

# Arena IDs
try:
    # Params Data
    subgraph = config['subgraph']['arena_api']
    tc_ids_query = config['query']['tc_ids_query']
    id_data_csv = config['files']['id_data']

    # Pulling Pair Data
    logger.info('TC ID Data Started')

    # Today and 2 Day Ago
    todayDate = datetime.now(timezone.utc)
    twodayago = todayDate - timedelta(2)
    my_time = datetime.min.time()
    my_datetime = datetime.combine(twodayago, my_time)
    timestamp = int(my_datetime.replace(tzinfo=timezone.utc).timestamp())
    tc_ids_query['variables']['startTime'] = timestamp

    # Pulling data from subgraph
    ids_df = pd.DataFrame()
    for i in itertools.count(0, 100):
        tc_ids_query['variables']['offset'] = i
        response = requests.post(url=subgraph, json=tc_ids_query)
        data = response.json()['data']['tradingCompetitions']

        if data == []:
            break
        else:
            temp_df = pd.json_normalize(data)
            ids_df = pd.concat([ids_df, temp_df], axis=0)
    ids_df.reset_index(drop=True, inplace=True)

    # Check if empty
    if ids_df.empty:
        raise Exception('Dataframe is empty')

    # Convert timestamp fields to datetime UTC and create new columns
    def convert_to_datetime(timestamp_str):
        return datetime.fromtimestamp(int(timestamp_str), tz=timezone.utc)

    timestamp_fields = ['timestamp.endTimestamp', 'timestamp.startTimestamp', 'timestamp.registrationStart', 'timestamp.registrationEnd']
    for field in timestamp_fields:
        new_col_name = field.replace('timestamp.', '') + '_datetime'
        ids_df[new_col_name] = ids_df[field].apply(convert_to_datetime)

    ids_df = ids_df.astype(str)
    
    # Comparing with current data
    ids_df_old = pd.read_csv(id_data_csv)
    ids_df_old['registrationStart_datetime'] = pd.to_datetime(ids_df_old['registrationStart_datetime'])
    timestamp_datetime = datetime.fromtimestamp(timestamp, tz=timezone.utc)
    drop_index = ids_df_old[ids_df_old['registrationStart_datetime']> timestamp_datetime].index
    index_list = drop_index.to_list()
    index_list = list(map(lambda x: x + 2, index_list))

    df_values = ids_df.values.tolist()

    # Check if empty
    if ids_df.empty:
        raise Exception('Dataframe is empty')

    # Write to GSheets
    credentials = os.environ['GKEY']
    credentials = json.loads(credentials)
    gc = gspread.service_account_from_dict(credentials)

    # Open a google sheet
    sheetkey = config['gsheets']['id_data_sheet_key']
    gs = gc.open_by_key(sheetkey)

    # Select a work sheet from its name
    worksheet1 = gs.worksheet('Master')
    if index_list != []:
        worksheet1.delete_rows(index_list[0], index_list[-1])

    # Append to Worksheet
    gs.values_append('Master', {'valueInputOption': 'USER_ENTERED'}, {'values': df_values})

    logger.info('TC ID Data Ended')
except Exception as e:
    logger.error('Error occurred during TC ID Data process. Error: %s' % e, exc_info=True)