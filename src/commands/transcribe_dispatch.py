import click
import os
from ..core.utils import timeit
import logging
import requests
import openai
import json
import pandas as pd
from pydub import AudioSegment
import time
from tqdm import tqdm

logger = logging.getLogger(__name__)


@click.command("transcribe_dispatch")
@click.pass_context


@click.option(
    "-d",
    "--data_path",
    default="data/outputs",
)
@click.option(
    "-o",
    "--open_ai_key",
    default="None",
)
@timeit

def transcribe_dispatch(ctx, data_path, open_ai_key):
    manifest_file = pd.read_csv(f"{data_path}/audio-manifest.csv")

 
    to_run = manifest_file[manifest_file['stt'] == False].sample(frac=1).copy()
  

    logger.warning(f"Total to Transcribe: {to_run.shape[0]}")
    logger.warning(f"Total  Transcribed: {manifest_file[manifest_file['stt'] == True].shape[0]}")


    for _, row in tqdm(to_run.iterrows(),total=to_run.shape[0]):
        openai.api_key = open_ai_key
        sound = AudioSegment.from_file(row['fpath'], "aac") 
        sound.export("data/audio/transcript.wav", format="wav")
        audio_file= open("data/audio/transcript.wav", "rb")
        transcript = openai.Audio.transcribe("whisper-1", audio_file)
        with open(f"{row['stt-path']}", "w") as outfile:
            json.dump(transcript, outfile)

