import logging
import os
import dagster as dg
import requests
import my_project.constants as constants

@dg.asset
def NYC_motor_vehicle_crashes(context: dg.AssetExecutionContext) -> dg.MaterializeResult: 
    # raw_data= requests.get(
    #     f"https://data.cityofnewyork.us/resource/h9gi-nx95.json$limit=999"
    # )

    # with open("output.json", "wb") as output_file:
    #     output_file.write(raw_data.content)
    logging.disable(logging.DEBUG)

    month_to_fetch = "2023-03"
    try:
        url = "https://data.cityofnewyork.us/api/v3/views/h9gi-nx95/query.csv?" \
        "pageSize=100&" \
        "pageNumber=1"

        payload = {}
        headers = {
        'X-App-Token': 'ypOu1S4FxLVgfEH7rzCKJg32m',
        }
        # Get the raw username and password from .env
        basic_auth_username = os.getenv('BASIC_AUTH_USERNAME')
        basic_auth_password = os.getenv('BASIC_AUTH_PASSWORD')

        response = requests.request(
            "GET",
            url, 
            headers=headers, 
            data=payload,
            auth=(basic_auth_username, basic_auth_password)
            )

        print(response.text)
    except requests.exceptions.RequestException as e:
        context.log.error(f"Failed to fetch data: {e}")
        raise
    
    try:
        with open(
            constants.CRASHES_FILE_PATH, "wb"
        ) as output_file:
            output_file.write(response.content)
    except IOError as e:
        context.log.error(f"Failed to write data to file: {e}")
        raise