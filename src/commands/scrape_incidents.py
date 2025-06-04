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


async def fetch(session, url, nid, data_path):
    async with session.get(url, timeout=100) as response:
        rtext = await response.text()
        async with aiofiles.open(f"{data_path}/logs/{nid}.txt", "w") as f:
            await f.write(rtext)



async def fetch_all(session, nids, data_path):
    tasks = []

    for nid in nids:
        url = f"https://data.sp0n.io/v1/incident/{nid}"
        task = asyncio.create_task(fetch(session, url, nid, data_path))
        tasks.append(task)

    results = await asyncio.gather(*tasks)

    return results


async def main(nids, data_path):
    async with aiohttp.ClientSession() as session:
        resp = await fetch_all(session, nids, data_path)
        # await write_geocodes(out_file_path, resp)



@click.command("scrape_incidents")

@click.pass_context

@click.option(
    "-d",
    "--data_path",
    help="Path to file with previously scraped locations",
    default="data",
)

@click.option(
    "-t",
    "--timeframe",
    default="all",
    help=""
    )


def scrape_incidents(ctx, data_path, timeframe):
    manifest_file = f"{data_path}/all-incidents-manifest.csv"

    all_incidents = pd.read_csv(manifest_file)
    all_incidents["t"] = (all_incidents["updatedAt"].apply(lambda x: dt.datetime.strptime(x, "20%y-%m-%dT%H:%M:%SZ")).copy())
    
    if timeframe != "all":
        to_run = all_incidents[all_incidents["t"] >= (dt.datetime.now() - dt.timedelta(hours=int(timeframe.replace("h",''))))].copy()
    else:
        to_run = all_incidents.copy()

    logger.warning(f"Total Logs in TimeFrame: {to_run.shape[0]} ")
    files=[]
    for filename in os.listdir(f"{data_path}/logs/"):
        f = os.path.join(f"{data_path}/logs/", filename)
        if f.endswith('txt'):
            files.append(f.split('/')[-1].replace('.txt',''))
    
    logger.warning(f"Total Logs Scraped: {len(files)}")

    to_run = to_run[~to_run["incidentId"].isin(files)]
    logger.warning(f"Total Logs To Run: {to_run.shape[0]} ")

    to_run = to_run["incidentId"].sample(frac=1).drop_duplicates().values

    chunk_size = 5
    
    for i in tqdm(range(0, len(to_run), chunk_size)):
        chunk = to_run[i : i + chunk_size]
        asyncio.get_event_loop().run_until_complete(main(chunk,data_path))

    # for index, row in all_incidents.iterrows():
    #     url = f"https://data.sp0n.io/v1/incident/{row['incidentId']}"
    #     updates = requests.get(url).json()
    #     for uid in updates["updates"]:
    #         update = updates["updates"][uid]
    #         update["cid"] = uid
    #         update["key"] = updates["key"]
    #         update["address"] = updates["address"]
    #         update["latitude"] = updates["latitude"]
    #         update["longitude"] = updates["longitude"]
    #         update["neighborhood"] = updates["neighborhood"]
    #         update["title"] = updates["title"]
    #         update["incident_ts"] = updates["ts"]
    #         update["police"] = updates["police"]
    #         gun_fire_incident_logs.append(update)

