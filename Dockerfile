# syntax=docker/dockerfile:1

# Comments are provided throughout this file to help you get started.
# If you need more help, visit the Dockerfile reference guide at
# https://docs.docker.com/go/dockerfile-reference/

# Want to help us make this template better? Share your feedback here: https://forms.gle/ybq9Krt8jtBL3iCk7

ARG PYTHON_VERSION=3.12.0
FROM python:${PYTHON_VERSION}-slim as base

# Prevents Python from writing pyc files.
ENV PYTHONDONTWRITEBYTECODE=1

# Keeps Python from buffering stdout and stderr to avoid situations where
# the application crashes without emitting any logs due to buffering.
ENV PYTHONUNBUFFERED=1
# From dotenv
ENV KEYWORD data
ENV LOCATION Remote
ENV API_KEY b5cdd88444c270c7fac125dc8199aa8b
ENV DAYS 2
ENV FOLDER Data
ENV TABLE_NAME JOB_WORDS_RAW
ENV SF_USER Kona
ENV PASSWORD ngFWnMT9cuG6XM6
ENV ACCOUNT bepcfpi-gf10139
ENV ROLE ACCOUNTADMIN
ENV WAREHOUSE LOADER
ENV DATABASE PC_DBT_DB
ENV SCHEMA RAW
ENV MATCH_WORDS Data/snow_words.csv
ENV MATCH_COLUMN_SKILLS Data_Skills
ENV MATCH_COLUMN_TECHNOLOGY Data_Technology
ENV DATA_FOLDER_PATH Data
ENV BACKUP_FOLDER Backups

# Install pip requirements
COPY requirements.txt .
RUN python -m pip install -r requirements.txt

WORKDIR /app
COPY . /app

# Create a non-privileged user that the app will run under.
# See https://docs.docker.com/go/dockerfile-user-best-practices/
ARG UID=10001
RUN adduser \
    --disabled-password \
    --gecos "" \
    --home "/nonexistent" \
    --shell "/sbin/nologin" \
    --no-create-home \
    --uid "${UID}" \
    appuser

# Download dependencies as a separate step to take advantage of Docker's caching.
# Leverage a cache mount to /root/.cache/pip to speed up subsequent builds.
# Leverage a bind mount to requirements.txt to avoid having to copy them into
# into this layer.
# RUN --mount=type=cache,target=/root/.cache/pip \
#     --mount=type=bind,source=requirements.txt,target=requirements.txt \
#     python -m pip install -r requirements.txt

# Switch to the non-privileged user to run the application.
USER appuser

# Copy the source code into the container.
# COPY . .

# Expose the port that the application listens on.
EXPOSE 8000

# Run the application.
CMD ["python", "__main__.py"]
