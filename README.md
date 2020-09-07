# Helios Server

This project is used to retrieve , display and store data coming in from weather stations.

## How to use it

#### Compiling

First you need to compile the project so that you end up with the application artifact.

`docker run -it --rm -v "$(pwd)":/app -w /app maven:3.6.3-jdk-8 mvn clean package`

#### Running it

`docker-compose up`

_**You can see the results of processing like this:**_

`docker-compose logs -t -f --tail 100 spark-primary`

#### Stopping the services

`docker-compose down` - This will also remove the containers.

##### Requirements

###### Fulfilled:

* One weather station sends us a stream of XMLs, containing the temperature of a given date in a given city measured on a particular timestamp. The temperature might be in celsius, fahrenheit or both.
  This information has to be consumed from a stream, stored and displayed in real-time with only latest temperatures (field measured_at_ts) in a given city (field city).
* Examples of XMLs are provided. Based on those you can generate incoming stream of data. New data can be generated every 10sec (whatever frequency).
* Errors (e.g. invalid xml, wrong input in measures) should be handled (program should not crash, data should be skipped but logged or stored separately). See attached XMLs 7 and 9 for examples.
* Displaying data should be in the console
* Storing data should be in a database or file(s)
* Temperature should be displayed both in celsius and fahrenheit for all incoming messages. Conversion rules can be found here.
Name of the city in incoming date, due to weather station issues, could contain different cases e.g. COPENHAGEN, CopEnhagen, however we want to display only in one case ‘Copenhagen’.
* Date in format YYYY-MM-DD should also be displayed as separate field.
* We added 2 more weather stations, generate and add 2 more streams of incoming with same message format.
* We want to display data that is not older than 3 days only.

####### Not Fulfilled

* We also want to display the country of the city. Either store in memory of your program a little map (CITY -> COUNTRY) or use one of the attached JSONs to map the city to its country.
* Add population information to the display of the data (to join with another stream of data, as more city-country relations might be added during program execution as the population evolves. See attached JSONs).
* We want Type 2 history stored for our transformed data. When building T2 history, we use measured_at_ts value for the history (active_from/active_to/active_flag fields). 
