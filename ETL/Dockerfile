FROM python:3.11-slim

# Set working directory inside the container
WORKDIR /app

# Install system dependencies needed for lxml and other packages
RUN apt-get update && apt-get install -y \
    gcc \
    libxml2-dev \
    libxslt-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements from ETL subfolder
COPY ETL/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy ETL source files
COPY ETL/ .

# Copy firestore_client.py from project root into the container
COPY firestore_client.py .

CMD ["python", "-c", "from pipeline import noaa_enso_etl_pipeline_flow; noaa_enso_etl_pipeline_flow()"]