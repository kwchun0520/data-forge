FROM --platform=linux/arm64 python:3.13-slim

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV DBT_PROFILES_DIR=/usr/app/dbt
ENV DEBIAN_FRONTEND=noninteractive

# Install system dependencies required for dbt-postgres on ARM64
RUN apt-get update && apt-get install -y \
    git \
    build-essential \
    libpq-dev \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Install dbt-postgres specifically for ARM64
RUN pip install --no-cache-dir \
    dbt-postgres==1.8.2 \
    psycopg2-binary

# Create working directory
WORKDIR /usr/app/dbt

# Verify installation
RUN dbt --version

# Set default command
CMD ["tail", "-f", "/dev/null"]