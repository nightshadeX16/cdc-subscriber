# Use an official Python runtime as a parent image
FROM python:3.10-slim

# Set environment variables
ENV APP_HOME /app
ENV PYTHONUNBUFFERED True

# Set the working directory in the container
WORKDIR $APP_HOME

# Install production dependencies
# Copy requirements first to leverage Docker cache
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the local code to the container
COPY . .

# Run the web server
CMD exec gunicorn --bind :$PORT --workers 1 --threads 8 main:app
