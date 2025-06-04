import click
import os
import pandas as pd
from ..core.utils import timeit
import logging
import requests
import datetime as dt
from ast import literal_eval

logger = logging.getLogger(__name__)


@click.command("check_audio_progress")
@click.pass_context


@click.option(
    "-d",
    "--data_path",
    help="Path to folder with the citizen_incidents",
    default="data/outputs",
)
@click.option(
    "-o",
    "--audio_path",
    help="Path to file with previously scraped locations",
    default="data/audio",
)
@timeit

def check_audio_progress(ctx, data_path, audio_path):
    incident_file = f"{data_path}/citizen_incidents.csv"
    STORAGE = audio_path

    try:
        
        incidents = pd.read_csv(incident_file)
        logger.warning(f"{incidents.shape[0]} Citizen Incidents")    

    except:
        logger.error(f"No Incidents Exists, Scrape Some Data or pull it from remote")
    
    
    # limit = [] 
    # likely_ss =  incidents[incidents['update_raw'].astype(str).str.contains('spotter',case=False)].copy()
    # gun_shot = incidents[incidents['update_raw'].astype(str).str.contains('gunshot',case=False)].copy()
    # gun_related = incidents[incidents['categories'].astype(str).str.contains('gun related',case=False)].copy()
    # police_related = incidents[incidents['categories'].astype(str).str.contains('Police Related',case=False)].copy()
    # lot_of_chatter = incidents[incidents['radioClips']>1]

    # incidents_of_interest = pd.concat([likely_ss,gun_shot,gun_related,police_related,lot_of_chatter]).drop_duplicates()

    audio=[]
    audio = [{"id": row['id'], "url": url} for _, row in incidents.iterrows() if row['radioClips'] != 0 for url in literal_eval(row['radioUrls'])]
    audio = pd.DataFrame(audio)
    audio_manifest = pd.DataFrame(audio)

    audio_manifest["fname"] = audio_manifest['id'] + "-" + audio_manifest['url'].apply(lambda x: x.split('/')[-1])
    audio_manifest["fpath"] = STORAGE + "/dispatch-audio/" + audio_manifest["fname"]
    audio_manifest["stt-path"] = STORAGE + "/dispatch-text/" + audio_manifest["fname"].apply(lambda x: x.split('.')[0]) + ".json"

    audio_manifest["downloaded"] = audio_manifest['fpath'].apply(lambda x: os.path.exists(x))
    audio_manifest["stt"] = audio_manifest['stt-path'].apply(lambda x: os.path.exists(x))

    
    logger.warning(f"Downloaded Audio: {audio_manifest[audio_manifest['downloaded']==True].shape[0]} Files")
    logger.warning(f"Speech to Text Audio: {audio_manifest[audio_manifest['stt']==True].shape[0]} Files")

    logger.warning(f"Writing Dispatch Audio Manifest to {data_path}/audio-manifest.csv")
    audio_manifest.to_csv(f"{data_path}/audio-manifest.csv",index=False)

