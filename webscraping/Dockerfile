# syntax=docker/dockerfile:1

# Start your image with a python base image
FROM python:3.11-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

# The /app directory should act as the main application directory
WORKDIR /usr/src/app

# Download dependencies as a separate step to take advantage of Docker's caching.
# Leverage a bind mount to requirements.txt to avoid having to copy it into this layer.
RUN --mount=type=bind,source=requirements.txt,target=requirements.txt \
    pip install --no-cache-dir -r requirements.txt

# Copy local directories to the current local directory of our docker image
COPY . .

# Run the application as a non-root user
RUN useradd -m scraper \
    && chown -R scraper:scraper /usr/src/app
USER scraper

# Run the python script
CMD ["python", "load_sanbox_data.py"]
