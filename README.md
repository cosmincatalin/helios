# Helios Server

This project is used to retrieve , display and store data coming in from weather stations.

## How to use it

### Dependencies

Before starting this service, it is best if you build and have running the `helios-station` services to publish weather data.
Instructions are attached in said project.

### Compiling

First you need to compile the project so that you end up with the application artifact.

`docker run -it --rm -v "$(pwd)":/app -w /app maven:3.6.3-jdk-8 mvn clean package`

### Running it

`docker-compose up --force-recreate`

_**You can see the results of processing like this:**_

`docker-compose logs -t -f --tail 100 spark`

#### Stopping the services

`docker-compose down --remove-orphans` - This will also remove the containers.

##### Requirements

###### Fulfilled:

* Retrieve, parse and display data coming from weather stations as `.xml` files on a `Kafka` stream.
* Create a separate application that is used in place of the weather stations to generate the `.xml` data.
* Gracefully handle invalid `.xml` files and faulty data, but log all data nevertheless.
* Display only the most up to date temperature and details for each city's weather station.
* Backup incoming data in a PostgreSQL database.
* Display temperature both in °C and in °F, either directly provided from the stream or inferred using formula.
* Display the city's name in a consistent manner by capitalizing.
* Display the timestamp as a formatted date.
* Support multiple weather stations from different cities.
* Display only data that is more recent than 3 days, while still backing up all data.
* Implement Type 2 history based on the timestamp of the data.

####### Not Fulfilled

* We also want to display the country of the city. Either store in memory of your program a little map (CITY -> COUNTRY) or use one of the attached JSONs to map the city to its country.
* Add population information to the display of the data (to join with another stream of data, as more city-country relations might be added during program execution as the population evolves. See attached JSONs). 
