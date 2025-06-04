import click
import os
import pandas as pd
from ..core.utils import timeit
import logging
import requests
from tqdm import tqdm
from datetime import datetime
import json

logger = logging.getLogger(__name__)


# Appends 

def get_times(x):
    try: 
        a = [str(datetime.fromtimestamp(int(str(update['ts'])[0:-3]))) for update in json.loads(x.replace("\'",'"')).values()] 
    except: 
        a = []
    return a

def get_seconds(x):
    try:
        datetimes = [datetime.strptime(dt_str, '%Y-%m-%d %H:%M:%S') for dt_str in x]

        # Sort the datetime objects
        sorted_datetimes = sorted(datetimes)
        time_diff = sorted_datetimes[-1] - sorted_datetimes[0]
        a = time_diff.seconds
    except:
        a = "Unparsed"
    return a

@click.command("compile_logs")
@click.pass_context


@click.option(
    "-d",
    "--data_path",
    help="Path to file with previously scraped locations",
    default="data/logs",
)
@click.option(
    "-o",
    "--out_path",
    help="Path to file with previously scraped locations",
    default="data/outputs",
)
@timeit

def compile_logs(ctx, data_path, out_path):
    logs = []
    LOG_DIR = data_path
    total = len(os.listdir(LOG_DIR))

    logger.warning(f"Logs to parse from {data_path}: {total}")

    for x in tqdm(os.listdir(LOG_DIR),total=total):
        if x.endswith(".txt"):
            with open(f"{LOG_DIR}/{x}") as f:
                try:
                    data = json.load(f)

                    log = {
                        'id': x.replace('.txt', ''),
                        'time': datetime.fromtimestamp(int(str(data['ts'])[0:-3])),
                        'citizen_link': f"https://citizen.com/{x.replace('.txt', '')}"
                    }

                    for key, value in data.items():
                        if key not in ['modules', 'updates']:
                            log[key] = value
                        elif key == 'updates':
                            log['number_of_updates'] = len(value)
                            log['update_raw'] = str(value)
                            log['update_text'] = " | ".join(update['text'] for update in value.values())
                            
                            for update in value:
                                t_u = value[update]
                                if "radioClips" in t_u.keys():
                                    log['radioClips'] = len(t_u['radioClips'])
                                    log['radioUrls'] = [clip['audioFileUrl'] for clip in t_u['radioClips']]
                                    try:
                                        log['transcriptions'] = [clip['transcription'] for clip in t_u['radioClips']]
                                        logger.warning(log['transcriptions'])
                                        
                                    except:
                                        log['transcriptions'] = []

                                else:
                                    log['radioClips'] = 0
                                    log['radioUrls'] = []
                                    log['transcriptions'] = []
                        

                    logs.append(log)
                except:
                    logger.warning(f"Issue with {x}")
    
    logs = pd.DataFrame(logs)

    logger.warning("APPENDING ADDITIONAL INFORMATION")

    logs['update_times'] = logs['update_raw'].apply(lambda x: get_times(x))
    logs['seconds'] = logs['update_times'].apply(lambda x: get_seconds(x))
    logs['api_link'] = "https://data.sp0n.io/v1/incident/" + logs['id']

    logs.to_csv(f"{out_path}/citizen_incidents.csv",index=False)


