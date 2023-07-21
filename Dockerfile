# Use the official Python image as a base image
FROM --platform=linux/amd64 python:3.11 

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app
# rename .env.example to .env
RUN mv .env.example .env

# Install required packages (including MySQL client and pm4py dependencies)
RUN apt-get update && apt-get install -y default-mysql-client && \
    pip install -r requirements.txt

# Expose the port the Flask app will be running on
EXPOSE 8087

# Start the Flask app
CMD ["python", "app.py"]
