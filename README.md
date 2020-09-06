# Helios Server

This project is used to retrieve , display and store data coming in from weather stations.

## How to use it

#### Compiling

First you need to compile the project so that you end up with the application artifact.

`docker run -it --rm -v "$(pwd)":/app -w /app maven:3.6.3-jdk-8 mvn clean package`

#### Running it

`docker-compose start`

You can see the results of processing like this:

`docker-compose logs -t -f --tail 100 spark-primary`

#### Stopping thhe service

`docker compose stop`
