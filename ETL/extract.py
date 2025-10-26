"""
Extracting 3-month rolling averages of ENSO magnitudes from 1950 to today, by scraping the information 
from NOAA's publicly available Oceanic Niño Index (ONI) webpage: 
https://www.cpc.ncep.noaa.gov/products/analysis_monitoring/ensostuff/ONI_v5.php
"""

import numpy as np 
import xarray as xr 
import requests 
from prefect import flow
from bs4 import BeautifulSoup  

@flow(name="scraping-noaa-oni-flow", retries=2)
def scrape_noaa_oni(url="https://cpc.ncep.noaa.gov/products/analysis_monitoring/ensostuff/ONI_v5.php", 
                    current_year=2025):
    page = requests.get(url) # Response 200 indicates we are permitted collect data from this website 
    # obtain page's information 
    soup = BeautifulSoup(page.text, 'lxml') 
    # obtain information from the webpage's primary table: tag <table align="center" border="1"> 
    main_table = soup.find('table', align='center', border='1') 

    # collecting all column, row, and ENSO event labels
    Table_Headers = [item.text for item in main_table.find_all('strong')]

    ONI = np.full(((current_year - 1950 + 1), 12), np.nan) 
    ENSO = [] 
    Year = [] 
    Season = [] 
    seasons = Table_Headers[1:13]

    # looping through NOAA webpage for ONI values and ENSO events 
    year_idx = 0 # year index 
    for row_idx in range(len(main_table.find_all('tr'))):  
        if row_idx == 0: 
            continue 
        else: 
            row_contents = main_table.find_all('tr')[row_idx] 
            if (row_contents.find(string='Year') == 'Year') == True: 
                continue # if row contains year and season info, skip it 
                
            # getting year from the first cell
            year_cell = row_contents.find('strong')
            if year_cell is None:
                continue
            year_value = year_cell.text

            # processing all 12 seasons within this year
            season_idx = 0 # season index 
            for cell_idx in range(1, 13): # seasons are in cells 1-12
                cells = row_contents.find_all('td', style="text-align:center;", width="7%")

                # check if the cell exists and has data
                if cell_idx < len(cells):
                    cell = cells[cell_idx]

                    # Check for colored spans (blue = La Niña, red = El Niño, black = Neutral)
                    blue_span = cell.find('span', style='color:blue')
                    red_span = cell.find('span', style='color:red')
                    black_span = cell.find('span', style='color:black')

                    if blue_span is not None:
                        # La Niña
                        ONI[year_idx, season_idx] = float(blue_span.text)
                        ENSO.append('LaNina')
                        Year.append(year_value)
                        Season.append(seasons[season_idx])
                    elif red_span is not None:
                        # El Niño
                        ONI[year_idx, season_idx] = float(red_span.text)
                        ENSO.append('ElNino')
                        Year.append(year_value)
                        Season.append(seasons[season_idx])
                    elif black_span is not None:
                        # Neutral
                        ONI[year_idx, season_idx] = float(black_span.text)
                        ENSO.append('Neutral')
                        Year.append(year_value)
                        Season.append(seasons[season_idx])
                    else:
                        # Cell exists but no data (incomplete year)
                        ENSO.append('Unknown')
                        Year.append(year_value)
                        Season.append(seasons[season_idx])
                    
                    season_idx += 1
            year_idx += 1

    # counting the number of times the latest year has data in main_table
    latest_year_count = len([item for item in Year if year_value in item])
    # adding info. for seasons with missing data (cell doesn't exist, incomplete year)
    if latest_year_count < 12:
        year_list = np.repeat(year_value, (12-latest_year_count)).tolist()
        enso_list = np.repeat('Unknown', (12-latest_year_count)).tolist()
        Year = Year + year_list
        ENSO = ENSO + enso_list
        Season = Season + Season[latest_year_count:12]

    # create xarray dataset
    ONI_xr = xr.Dataset( 
        { 
            "ONI": (["year","season"], ONI), 
        }, 
        coords={ 
            "year": np.arange(1950, current_year + 1), 
            "season": Season[0:12], 
            "ENSO": (["year","season"], np.reshape(ENSO, ONI.shape)), 
        },
        attrs={
            "source": "NOAA Climate Prediction Center",
            "url": url,
            "description": "Oceanic Niño Index (ONI) - 3-month rolling average of SST anomalies",
            "last_updated": f"{np.datetime64('now')} UTC"
        }
    ) 

    return ONI_xr