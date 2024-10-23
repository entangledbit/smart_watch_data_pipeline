# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /usr/src/app

# Copy the current directory contents into the container
COPY . .

# Install the necessary packages
RUN pip install --no-cache-dir -r requirements.txt

# Set the entrypoint to the Python script but allow overriding both the server URL and speed factor
ENTRYPOINT ["python", "streaming_client.py", "data.json"]

# Default arguments (can be overridden at runtime)
CMD ["--server_url", "http://localhost:62333/stream", "--speed", "1.0"]