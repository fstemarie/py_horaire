# Use the official Python image from the Docker Hub
FROM docker.io/python:3.12.8-alpine3.21

LABEL org.opencontainers.image.source https://github.com/fstemarie/horaire.git

# Set the working directory in the container
WORKDIR /app/horaire

# Copy the current directory contents into the container at /app/horaire
COPY ./src /app/horaire
COPY ./requirements.txt /app/horaire

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Define environment variable
ENV NAME horaire
ENV HORAIRE_WORKSPACE /workspace

VOLUME /workspace
