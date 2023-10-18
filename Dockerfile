# Use the official Python image as a base image
FROM --platform=linux/amd64 python:3.11

# Set the working directory in the container
WORKDIR /app

# Copy the current directory contents into the container at /app
COPY . /app
# rename .env.example to .env
RUN mv .env.example .env

# Install required packages (including MySQL client and pm4py dependencies)
RUN apt-get update && apt-get install -y default-mysql-client 
RUN pip install -r requirements.txt

RUN pip install gunicorn

# run as privileged user
USER root

# Expose the port the Flask app will be running on
EXPOSE 8087

# Start the Flask app using the gunicorn WSGI server
CMD ["gunicorn", "app:app", "-b", "0.0.0.0:8087"]
