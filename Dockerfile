# Use the full Python image (Debian base) for better compatibility
FROM python:3.10 

# Set the working directory in the container
WORKDIR /app

# Install system dependencies needed specifically for PostgreSQL (libpq-dev)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    libpq-dev && \
    rm -rf /var/lib/apt/lists/*

# Install required Python libraries
RUN pip install --no-cache-dir \
    psycopg2-binary \
    sqlalchemy \
    sqlalchemy-utils \
    streamlit \
    plotly

# Copy the local code (etl_scripts, dashboard, and data directories) into the container
COPY . /app

# Expose the port used by Streamlit (8501)
EXPOSE 8501