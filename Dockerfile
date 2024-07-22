# Use an official Python runtime as a parent image
FROM python:3.9-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

# Set the working directory in the container
WORKDIR /code

# Install dependencies
COPY requirements.txt /code/
RUN pip install --upgrade pip
RUN pip install -r requirements.txt

# Copy the current directory contents into the container at /code/
COPY . /code/

# Expose port 8000 to the outside world
EXPOSE 8000

# Run Django application
CMD ["python", "manage.py", "runserver", "0.0.0.0:8000"]


docker login -u AWS -p $(aws ecr get-login-password --region  ap-south-1) 732165046977.dkr.ecr.ap-south-1.amazonaws.com


this is ~/.aws/credentials 
and aws ecr --region --profile default  | docker login --username AWS --password-stdin 732165046977.dkr.ecr.ap-south-1.amazonaws.com this is the command its showing 

arshil.khan@192 stock_comparison_project % aws ecr --region ap-south-1 --profile default  | docker login --username AWS --password-stdin 732165046977.dkr.ecr.ap-south-1.amazonaws.com

usage: aws [options] <command> <subcommand> [<subcommand> ...] [parameters]
To see help text, you can run:

  aws help
  aws <command> help
  aws <command> <subcommand> help

aws: error: the following arguments are required: operation

Error: Cannot perform an interactive login from a non TTY device