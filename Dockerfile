#Dockerfile

#pull offical python image
FROM python:3.9.4-slim

#assign work directory
WORKDIR /app

#Set enviormental variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNUFFERED 1

# intall package dependecies
COPY requirements.txt .
RUN pip install -r requirements.txt

# copy project
COPY . .