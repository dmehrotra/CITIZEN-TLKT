import click
from ..core.utils import timeit
import os
import aiohttp
import asyncio
import aiofiles
from tqdm import tqdm
import pandas as pd
import random


def get_jobs(places, cache):
    jobs = []
    for t in places:
        if t[2] == False:
            if f"{t[0]}-{t[1]}" not in cache:
                jobs.append(t)
        else:
            if f"{t[2]}" not in cache:
                jobs.append(t)

    return jobs


def get_coords(filepath):
    coords = []
    with open(filepath, mode="r") as f:
        for line in f.readlines():
            if not "lat" in line:
                cols = line.strip().split(",")
                if cols[0].strip() == "" or cols[1].strip() == "" or cols[0] == "False":
                    continue
                coords.append((cols[0], cols[1], False))
                # [float(l) for l in]
    return coords


def format_cache(out_file_path):
    df = pd.read_csv(out_file_path)
    df = df.loc[df["success"] == True]
    for c in ["lat", "lon"]:
        df[c] = df[c].astype(float)
        df[c] = df[c].apply(lambda x: round(x, 5))
    for c in ["geoid", "block", "block_group", "tract", "county", "state"]:
        df[c] = df[c].astype(int)
    df = df.drop_duplicates()
    df.to_csv(out_file_path, index=False)


def get_addresses(filepath):
    addresses = []
    with open(filepath, mode="r") as f:
        for line in f.readlines():
            if not "lat" in line:
                cols = line.strip().split(",")
                if cols[2] != "False":
                    addresses.append((False, False, ",".join(cols[2:])))
                # [float(l) for l in]
    return addresses


async def write_geocodes(out_file_path, data):
    async with aiofiles.open(out_file_path, "a") as f:
        for d in data:
            await f.write(
                f"{d['success']},{d['lat']},{d['lon']},{d['address']},{d['geoid']},{d['block']},{d['block_group']},{d['tract']},{d['county']},{d['state']},{d['county_name']},{d['state_name']},{d['match_type']}\n"
                )


async def fetch(session, url, lat, lon, addr):
    async with session.get(url, timeout=100) as response:

        try:
            body = await response.json()
            obj = {}
            if "address" not in url:
                block_level = body["result"]["geographies"]["Census Blocks"][0]
                states_level = body["result"]["geographies"]["States"][0]
                counties_level = body["result"]["geographies"]["Counties"][0]
                obj["vintage_name"] = body["result"]["input"]["vintage"]["vintageName"]
                obj["lat"] = lat
                obj["lon"] = lon
                obj["address"] = addr
                obj["geoid"] = block_level["GEOID"]
                obj["block"] = block_level["BLOCK"]
                obj["block_group"] = block_level["BLKGRP"]
                obj["tract"] = block_level["TRACT"]
                obj["county"] = block_level["COUNTY"]
                obj["state"] = block_level["STATE"]
                obj["county_name"] = counties_level["NAME"]
                obj["state_name"] = states_level["NAME"]
                obj["success"] = True
                obj["match_type"] = "coordinates"
            else:
                result = body["result"]["addressMatches"][0]
                block_level = result["geographies"]["Census Blocks"][0]
                states_level = result["geographies"]["States"][0]
                counties_level = result["geographies"]["Counties"][0]
                obj["vintage_name"] = body["result"]["input"]["vintage"]["vintageName"]
                obj["lat"] = result["coordinates"]["y"]
                obj["lon"] = result["coordinates"]["x"]
                obj["address"] = addr
                obj["geoid"] = block_level["GEOID"]
                obj["block"] = block_level["BLOCK"]
                obj["block_group"] = block_level["BLKGRP"]
                obj["tract"] = block_level["TRACT"]
                obj["county"] = block_level["COUNTY"]
                obj["state"] = block_level["STATE"]
                obj["county_name"] = "Unparsed"
                obj["state_name"] = "Unparsed"
                obj["success"] = True
                obj["match_type"] = "address"

            return obj

        except Exception as identifier:
            obj = {}
            obj["vintage_name"] = ""
            obj["lat"] = lat
            obj["lon"] = lon
            obj["address"] = addr
            obj["geoid"] = ""
            obj["block"] = ""
            obj["block_group"] = ""
            obj["tract"] = ""
            obj["county"] = ""
            obj["state"] = ""
            obj["county_name"] = ""
            obj["state_name"] = ""
            obj["success"] = False
            obj["match_type"] = False
            return obj


async def get_zipcode(session, url, geocoded_result):
    geocoded_result["zip"] = "12508"
    return geocoded_result


async def fetch_all(session, places, vintage="410"):
    geocode_tasks = []
    zipcode_tasks = []

    for lat, lon, addr in places:
        # vintage 410 - 2010 Census Blocks
        # vintage 420 - Current_Current (2020)
        if addr == False:
            url = f"https://geocoding.geo.census.gov/geocoder/geographies/coordinates?x={lon}&y={lat}&benchmark=4&vintage={vintage}&format=json"
            task = asyncio.create_task(fetch(session, url, lat, lon, addr))
        else:
            try:
                street, city, state = addr.replace('"', "").split(",")
                url = f"https://geocoding.geo.census.gov/geocoder/geographies/address?street={street}&city={city}&state={state}&benchmark=4&vintage={vintage}&format=json"
                task = asyncio.create_task(fetch(session, url, lat, lon, addr))
            except:
                continue

        geocode_tasks.append(task)

    geocoded_results = await asyncio.gather(*geocode_tasks)

    return geocoded_results


# async def fetch_all(session, coords, vintage="410"):
#     tasks = []
#     for lat, long in coords:
#         # vintage 410 - 2010 Census Blocks
#         # vintage 420 - Current_Current (2020)
#         url = f"https://geocoding.geo.census.gov/geocoder/geographies/coordinates?x={long}&y={lat}&benchmark=4&vintage={vintage}&format=json"
#         task = asyncio.create_task(fetch(session, url, lat, long))
#         tasks.append(task)
#     results = await asyncio.gather(*tasks)
#     return results

#     geocoded_results = await asyncio.gather(*geocode_tasks)


async def main(coords, out_file_path, vintage):
    async with aiohttp.ClientSession() as session:
        resp = await fetch_all(session, coords, vintage)
        await write_geocodes(out_file_path, resp)


header = "success,lat,lon,address,geoid,block,block_group,tract,county,state,county_name,state_name,match_type\n"


@click.command("geocode")
@click.pass_context
@click.option(
    "-i",
    "--in_file_path",
    help="File with coordinates",
)
@click.option(
    "-v",
    "--vintage",
    default="410",
    help="vintage 410 - 2010 Census Blocks, vintage 420 - Current_Current (2020)",
    type=click.Choice(["410", "420"]),
)
@click.option(
    "-t",
    "--type",
    default="census",
    help="",
    type=click.Choice(["census", "google"]),
)
@timeit
def geocode(ctx, in_file_path, vintage,type):
    """Get census geocode info for the coordinates"""
    data_dir = ctx.obj["base"]

    out_file_path = os.path.join(
        data_dir,
        in_file_path.split("/")[1],
        in_file_path.split("/")[-1].replace(".csv", "_geocoded.csv"),
    )
    cache = set()
    if not os.path.exists(out_file_path):
        with open(out_file_path, mode="w") as f:
            f.write(header)
    else:
        format_cache(out_file_path)

        with open(out_file_path, mode="r") as f:
            for line in f.readlines():
                t = line.strip().split(",")
                if t[-1] == "address":
                    cache.add(f"{t[3]},{t[4]},{t[5]}")
                else:
                    cache.add(f"{t[1]}-{t[2]}")

    places = get_coords(in_file_path)
    if type != 'google':
        places += get_addresses(in_file_path)

    to_run = get_jobs(places, cache)
    random.shuffle(to_run)

    # print(len(to_run))
    # asyncio.get_event_loop().run_until_complete(
    #     main(to_run[:10], out_file_path, vintage)
    # )

    chunk_size = 1
    for i in tqdm(range(0, len(to_run), chunk_size)):
        chunk = to_run[i : i + chunk_size]
        asyncio.get_event_loop().run_until_complete(main(chunk, out_file_path, vintage))
