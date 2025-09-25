import io
import logging
import os
import pandas as pd
import dagster as dg
import requests
import my_project.constants as constants


@dg.asset
def nyc_motor_vehicle_crashes(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    # raw_data= requests.get(
    #     f"https://data.cityofnewyork.us/resource/h9gi-nx95.json$limit=999"
    # )

    # with open("output.json", "wb") as output_file:
    #     output_file.write(raw_data.content)
    logging.disable(logging.DEBUG)

    # month_to_fetch = "2023-03"  # Not currently used, keeping for future reference
    try:
        base_url = "https://data.cityofnewyork.us/api/v3/views/h9gi-nx95/query.csv"
        page_size = 10000  # Maximum allowed by the API
        page_number = 1    # 0-based page number for the API
        all_dfs = []
        total_records = 0
        
        app_token = os.getenv("APP_TOKEN")
        headers = {"X-App-Token": app_token} if app_token else {}
        
        # Get the basic auth credentials from .env (optional)
        basic_auth_username = os.getenv("BASIC_AUTH_USERNAME")
        basic_auth_password = os.getenv("BASIC_AUTH_PASSWORD")
        auth = (basic_auth_username, basic_auth_password) if basic_auth_username and basic_auth_password else None
        
        context.log.info("Starting to fetch data with pagination...")
        
        while True:
            # Update URL with current page number
            url = f"{base_url}?pageSize={page_size}&pageNumber={page_number}"
            
            response = requests.request(
                "GET",
                url,
                headers=headers,
                auth=auth,
                timeout=30
            )
            
            response.raise_for_status()
            
            # Read the CSV response into a DataFrame using StringIO
            df = pd.read_csv(io.StringIO(response.text))
            records_in_page = len(df)
            all_dfs.append(df)
            total_records += records_in_page
            
            context.log.info(f"Fetched page {page_number} with {records_in_page} records")
            
            # Check if we've received fewer records than the page size (last page)
            if records_in_page < page_size:
                break
                
            page_number += 1
        
        # Concatenate all DataFrames
        if all_dfs:
            combined_df = pd.concat(all_dfs, ignore_index=True)
            context.log.info(f"Successfully fetched {total_records} total records in {page_number} pages")
        
    except requests.exceptions.RequestException as e:
        context.log.error(f"Failed to fetch data: {e}")
        raise

    try:
        # Save the DataFrame to a CSV file
        combined_df.to_csv(constants.CRASHES_FILE_PATH, index=False)
        context.log.info(f"Successfully saved {len(combined_df)} records to {constants.CRASHES_FILE_PATH}")
    except Exception as e:
        context.log.error(f"Failed to save data to file: {e}")
        raise
