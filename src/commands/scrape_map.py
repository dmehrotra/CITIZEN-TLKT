import click
import os
import pandas as pd
from ..core.utils import timeit
import logging
import requests
import datetime as dt
from tqdm import tqdm

logger = logging.getLogger(__name__)


def get_incidents(service_areas):
    incidents = []
    for area in tqdm(service_areas["Service Area"].values,total=len(service_areas["Service Area"].values)):
        url = f"https://data.sp0n.io/v1/homescreen/mapIncidents?serviceAreaCode={area}"
        events = requests.get(url).json()
        for kind in ["incidents", "inactiveIncidents"]:
            for incident in events[kind]:
                incident["area"] = area
                incidents.append(incident)
    
    return pd.DataFrame(incidents)


@click.command("scrape_map")
@click.pass_context

@click.option(
    "-d",
    "--data_path",
    help="Path to file with previously scraped locations",
    default="data",
)

@timeit


def scrape_map(ctx, data_path):
    manifest_file = f"{data_path}/all-incidents-manifest.csv"
    sa_file = f"{data_path}/service-areas-{dt.date.today()}.csv"
    
    incident_manifest = pd.read_csv(manifest_file)

    params = (("lowerLongitude", "-136.642239"),("lowerLatitude", "29.120548"),("upperLongitude", "-64.12608480632346"),("upperLatitude", "54.587296784039594"))
    response = requests.get("https://data.sp0n.io/v1/homescreen/mapExplore", params=params)
   
    service_areas = pd.DataFrame(response.json()["serviceAreas"])
    service_areas.columns = ["Service Area"]
    service_areas['Date'] = dt.date.today()

    logger.warning(f"Service Areas: {service_areas.shape[0]}")
    service_areas.to_csv(sa_file,index=False)

    incidents = get_incidents(service_areas)
    logger.warning(f"Incidents visible on map: {len(incidents)}")
    
    new_incidents_not_in_manifest = incidents[
        ~incidents["incidentId"].isin(incident_manifest["incidentId"])
    ]

    
    all_incidents = pd.concat([incident_manifest, new_incidents_not_in_manifest])
    all_incidents["url"] = "https://citizen.com/" + all_incidents["incidentId"]
    all_incidents.to_csv(manifest_file, index=False)

    updated_incident_manifest = pd.read_csv(manifest_file)
    
    logger.warning(f"Previous Manifest File: {incident_manifest.shape[0]} Incidents")    
    logger.warning(f"Current  Manifest File: {updated_incident_manifest.shape[0]} Incidents")    



