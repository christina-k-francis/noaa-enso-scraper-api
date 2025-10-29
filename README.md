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
data = response.json()
pretty_json = json.dumps(data, indent=2)
print(pretty_json)
