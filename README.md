# NOAA ENSO Scraper API
This is a simple API that provides all Oceanic Niño Index (ONI) data as calculated by the NOAA NCEP. 
It then enhances the basic information provided by NOAA with derived statistics including:
- ENSO phase duration
- Intensity classification
- Rate of change of ONI (dONI/dt)
- 25th and 75th percentile rankings based on all historical data

# Usage
## No API Key Required
A key is not required to utilize this API.

## Free
This API functions on the usage of public domain data, and thus is completely free to use.

## Example Use:
```python
import requests
import json

API_URL = "https://noaa-enso-scraper-server.onrender.com"

endpoint = "/oni/episodes"

params = {
    "enso_type": "LaNina",      # Filter for La Niña events only
    "min_duration": 24          # Only events lasting 24+ months
}

response = requests.get(f"{API_URL}{endpoint}", params=params)
data = response.json() ```

# Feedback
If you have any questions, recommendations for improvements, or bugs to report, please let us know using [this form](https://docs.google.com/forms/d/e/1FAIpQLSf__Rann5R-2MM_GBGWHl5UOHjWZ3BKszjnfD4RIdiCt_L6SA/viewform?usp=sharing&ouid=105089325141011824344).
Thanks for giving this a try!
pretty_json = json.dumps(data, indent=2)
print(pretty_json)
