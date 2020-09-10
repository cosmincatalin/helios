# Helios Server

This project is used to retrieve , display and store data coming in from weather stations.

## How to use it

### Dependencies

Before starting this service, it is best if you build and have running the `helios-station` services to publish weather data.
Instructions are attached in said project.

### Building

First you need to build the project so that you end up with the application artifact.

`docker run -it --rm -v "$(pwd)":/app -w /app maven:3.6.3-jdk-8 mvn clean package`

### Running it

`docker-compose up --force-recreate`

_**You can see the results of processing like this:**_

`docker-compose logs -t -f --tail 100 spark-client`

### Stopping the services

`docker-compose down --remove-orphans` - This will also remove the containers.

### Configuration

The services can be configured via the Docker images they run on using environment variables.

### Notes

* The streaming populations feature can be tested by placing a new valid `.json` file along the others in the `data` folder.
* Depending on your Docker configuration and resource allocation, the application might take some time before it shows the first results on the console.  
