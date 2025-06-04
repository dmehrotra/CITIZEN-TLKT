import click
import os
import aiohttp
import asyncio
import aiofiles
from tqdm import tqdm
import pandas as pd
import logging
import datetime as dt

logger = logging.getLogger(__name__)


async def fetch(session, url, audio_path):
    async with session.get(url, timeout=100) as response:
        if response.status == 200:
            async with aiofiles.open(f"{audio_path}", "wb") as f:
                await f.write(await response.read())
                await f.close()



async def fetch_all(session, chunk):
    tasks = []

    for _, row in chunk.iterrows():
        url = row['url']
        audio_path = row['fpath']
        task = asyncio.create_task(fetch(session, url, audio_path))
        tasks.append(task)

    results = await asyncio.gather(*tasks)

    return results


async def main(chunk):
    async with aiohttp.ClientSession() as session:
        resp = await fetch_all(session, chunk)



@click.command("pull_dispatch")

@click.pass_context

@click.option(
    "-d",
    "--data_path",
    help="Path to file with previously scraped locations",
    default="data/outputs",
)

@click.option(
    "-l",
    "--limit",
    default="all",
    help=""
    )



def pull_dispatch(ctx, data_path, limit):
    try:
        manifest_file = pd.read_csv(f"{data_path}/audio-manifest.csv")
    except:
        logger.error("Did you generate an audio manigfest using check_audio_progress.py")


    if limit != "all":
        to_run = manifest_file[manifest_file['downloaded'] == False].sample(int(limit)).copy()
    else:
        to_run = manifest_file[manifest_file['downloaded'] == False].copy()

    logger.warning(f"Total to Download: {to_run.shape[0]}")
    logger.warning(f"Total  Downloaded: {manifest_file[manifest_file['downloaded'] == True].shape[0]}")

    chunk_size = 10
    
    for i in tqdm(range(0, to_run.shape[0], chunk_size)):
        chunk = to_run[i : i + chunk_size]
        asyncio.get_event_loop().run_until_complete(main(chunk))



