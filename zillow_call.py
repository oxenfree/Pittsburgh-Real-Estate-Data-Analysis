#!pip install python-zillow
import zillow
from zillow import ZillowError
from local import *
import glob
from datetime import datetime
import json
import pandas as pd
import os

zapi_key = ZILLOW_API_KEY
zapi = zillow.ValuationApi()
pwd = os.getcwd()


class DataIO:
    @staticmethod
    def read_files():
        encyclopedia = {
            'good_calls': {
                'path': f'{pwd}/data/good_calls/'
            },
            'bad_calls': {
                'path': f'{pwd}/data/bad_calls/'
            },
        }
        already_called = []
        
        for name, dictionary in encyclopedia.items():
            df_path = dictionary['path']
            if os.path.isdir(df_path):
                for file in glob.glob(f'{df_path}/*'):
                    df = pd.read_csv(file, index_col=0)
                    try:
                        dictionary['frame'] = pd.concat([dictionary['frame'], df])
                    except KeyError:
                        dictionary['frame'] = df
                    called = df.parid.tolist()
                    if len(called):
                        already_called.extend(called)
                        
        encyclopedia['already_called'] = already_called

        return encyclopedia

    @staticmethod
    def write_data_files(bad_calls, house_details):
        now = datetime.now()
        right_now = f'{now.year}_{now.month}_{now.day}_{now.hour}_{now.minute}'
        bad_calls_dir = f'{pwd}/data/bad_calls/'
        good_calls_dir = f'{pwd}/data/good_calls/'
        for path in [bad_calls_dir, good_calls_dir]:
            if not os.path.isdir(path):
                os.makedirs(path)
        bad_calls_today = pd.DataFrame(
            {
                'parid': [key for key in bad_calls.keys()],
                'address': [add for add in bad_calls.values()]
            }
        )
        bad_calls_today.to_csv(f'data/bad_calls/{right_now}.csv')
        good_calls = pd.DataFrame(house_details).T.reset_index().rename(columns={'index': 'parid'})
        good_calls.to_csv(f'data/good_calls/{right_now}.csv')

        return good_calls

    
    def call_zillow(self, whole_enchilada, is_test=None):
        encyclopedia = self.read_files()
        already_called = encyclopedia['already_called']
        if is_test:
            return encyclopedia
        year = datetime.now().year + 1  # we're adding one year because it's Dec 2018
        api_limit = 4500  # <- frowny face emoji
        count_down = api_limit
        house_details = {}
        bad_calls = {}
        for row in whole_enchilada.iterrows():
            row_id = row[1].parid
            if row_id in already_called:
                continue
            count_down -= 1
            try:
                house = zapi.GetDeepSearchResults(zapi_key, row[1].street_address, row[1].zip_code)
            except ZillowError:
                print(f'. {api_limit - count_down}')
                bad_calls[row_id] = f'{row[1].street_address}, {row[1].zip_code}'
                continue
            ext_data = house.get_dict()['extended_data']
            address = house.get_dict()['full_address']
            age = ''
            if ext_data['year_built'] is not None:
                age = year - int(ext_data['year_built'])
            house_details[row_id] = {
                'use_code': ext_data['year_built'],  # lol, dummy. found this too late
                'year_built': ext_data['year_built'],
                'age': age,
                'lot_size': ext_data['lot_size_sqft'],
                'sqft': ext_data['finished_sqft'],
                'baths': ext_data['bathrooms'],
                'beds': ext_data['bedrooms'],
                'lng': address['longitude'],
                'lat': address['longitude']  # lol, dummy. found this too late
            }
            if count_down % 200 == 0:
                display(house_details[row_id])
            if count_down <= 1:
                break
        good_calls = self.write_data_files(bad_calls, house_details)
        
        return pd.concat([good_calls, encyclopedia['good_calls']['frame']])
