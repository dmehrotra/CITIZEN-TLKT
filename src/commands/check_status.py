import click
import os
import pandas as pd
from ..core.utils import timeit
import logging
import requests
import datetime as dt

logger = logging.getLogger(__name__)


@click.command("check_status")
@click.pass_context


@click.option(
    "-d",
    "--data_path",
    help="Path to file with previously scraped locations",
    default="data",
)
@timeit

def check_status(ctx, data_path):
    manifest_file = f"{data_path}/all-incidents-manifest.csv"
    try:
        
        incident_manifest = pd.read_csv(manifest_file)
        logger.warning(f"{incident_manifest.shape[0]} Citizen Incidents")    

    except:
        logger.error(f"No Manifest File Exists, Ask Dhruv for it")
    
    try:
        
        params = (("lowerLongitude", "-136.642239"),("lowerLatitude", "29.120548"),("upperLongitude", "-64.12608480632346"),("upperLatitude", "54.587296784039594"))
        response = requests.get("https://data.sp0n.io/v1/homescreen/mapExplore", params=params)
        service_areas = response.json()["serviceAreas"]
        logger.warning(f"Service Areas: {len(service_areas)}")

    except:
        logger.error("Error with: https://data.sp0n.io/v1/homescreen/mapExplore")
        

    logger.warning("API is working")