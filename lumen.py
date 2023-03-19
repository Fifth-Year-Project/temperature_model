import os
import json
import pickle
import sys
import configparser
import redis # Installed in all Lumen containers by default.
import simplejson as json
__has_init__ = False

def _save_to_redis(result: dict):
    """
    Save a result dictionary to Redis
​
    :param result: Result dictionary to be saved to Redis.
    """
    r =  redis.Redis(os.getenv('REDIS_ADDR'))
    r.set(sys.argv[1], json.dumps(result, default = str, ignore_nan=True))
    
def save(result: dict):
    """
    Save results in a Python dictionary to Lumen.
​
    :param result: Result dictionary to be saved to Lumen.
    """
    try:
        _save_to_redis(result)
    except:
        print("Could not connect to redis instance, writing to stderr instead.", file=sys.stderr)
        print(json.dumps(result, indent=4, sort_keys=True))
        
def save_dataframe(df, output_name: str):
    """
    Save a Pandas DataFrame to Lumen. Only accepts one DataFrame to be saved.
​
    :param df: DataFrame to save to Lumen.
    :param output_name: Output name to be used in Lumen.
    """
    import pandas as pd
    
    if not isinstance(df, pd.DataFrame):
        save_exception("save_dataframe function was not passed a Pandas DataFrame.")
        
    try:
        _save_to_redis({output_name: df.to_dict(orient='records')})
    except:
        filename = get_filepath(f"{output_name}.csv", location="agent")
        df.to_csv(filename)

def save_exception(message: str):
    """
    Save an exception message to be displayed in case of an error.
​
    :param message: Message to be displayed on error.
    """
    print(message)
    save({
        "exception": message,
    })
    sys.exit(1)

def _persistent_filepath(filename: str):
    """
    Generate the filepath for a given filename to be stored in persistent storage.
    
    :param filename: The desired filename. Must include file extension.
    :returns A filepath in persistent storage.
    """
    try:
        __location__ = os.getenv('PERSISTENT_DATA_PATH', './')
    except EnvironmentError: # is this the right error?
        print("Persistent storage location not found in environment variables. Saving to current directory instead.")
        return _agent_filepath(filename)
    return os.path.join(__location__, filename)


def _agent_filepath(filename: str):
    """
    Generate the filepath for a given filename to be stored in agent storage.
    
    :param filename: The desired filename. Must include file extension.
    :returns A filepath in agent storage.
    """
    __location__ = os.path.realpath(os.path.join(os.getcwd(), os.path.dirname(__file__)))
    return os.path.join(__location__, filename)

def get_filepath(filename:str, location:str="agent"):
    """
    Get a filepath to store a file in Lumen.
​
    :param filename: The desired filename. Must include file extension.
    :param location: Where the file should be stored. May be "agent" or "persistent".
    :returns A file location
    """
    if location == "agent":
        return _agent_filepath(filename)
    elif location == "persistent":
        return _persistent_filepath(filename)
    else:
        save_exception(f"get_filepath not implemented for location={location}")

def disk_persist(filename:str, value:object, location:str="agent"):
    filepath = get_filepath(filename, location)
    with open(filepath, 'wb+') as f:
        pickle.dump(value, f)

def disk_load(filename:str, location:str="agent"):
    """
    Read a file from Agent or Persistent storage on Lumen.
    - Agent storage contains files uploaded with an Agent.
    - Persistent storage should be used to store agent state between executions.
​
    :param name: The name of the file to be read, including any file extension.
    :param location: The location to be read from. Either "agent" or "persistent". Default to "agent".
    """
    filepath = get_filepath(filename, location)
    if os.path.exists(filepath):
        with open(filepath, 'rb') as f:
            return pickle.load(f)

def consolidate_output_feed(endpoint:str, access_key:str, limit:int=1):
    """
    Consolidate data from an existing output feed on Lumen.
    
    :param endpoint: Endpoint asssociated with the Output Feed
    :param access_key: Access key for the specified Output Feed
    :param limit: The number of results to return
    :returns A list of execution results for the specified Output Feed details
    """
    import requests
    data = {
        "access_key": access_key,
        "limit": limit,
        }
    r = requests.post(url=endpoint, data=json.dumps(data)).json()
    return [x["execution-results"] for x in r]

def consolidate_csv_data_manager(endpoint: str, access_key: str):
    """
    Consolidate the latest output from a CSV Data Manager on Lumen.
​
    :param endpoint: Endpoint of the specified Output Feed
    :param access_key: Access key of the specified Output Feed
    :returns A Pandas DataFrame of the latest upload to the specified CSV Data Manager
    """
    import pandas as pd
    data = consolidate_output_feed(endpoint, access_key, limit=1)[0]["csv_output"]
    return pd.DataFrame(data)

if __has_init__ == False:
    __has_init__ = True

    environFilename = get_filepath('environ.ini', location="agent")
    if os.path.exists(environFilename):
        parser = configparser.ConfigParser()
        parser.read(environFilename)
        if parser.has_section('lumen'):
          for k, v in parser['lumen'].items():
              os.environ[k.upper()] = v