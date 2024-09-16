# Use an official Python runtime as a parent image
FROM python:3.11.9-slim

# Set the working directory in the container
WORKDIR /app

COPY . /app

RUN pip install --no-cache-dir --upgrade pip
RUN pip install -r requirements.txt

# Make port 5050 available to the world outside this container
EXPOSE 8080

# Run api.py when the container launches
CMD python -m pipeline.py