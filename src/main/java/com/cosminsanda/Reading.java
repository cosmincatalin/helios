package com.cosminsanda;


import lombok.Getter;
import lombok.Setter;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.stream.Collectors;

public class Reading {
    @Getter private Timestamp timestamp = null;
    @Getter private String city = null;
    @Getter private Double celsius = null;
    @Getter private Double fahrenheit = null;
    @Getter @Setter private String country = null;

    public Reading() {
    }

    public void setCity(String city) {
        this.city = Arrays.stream(city.split(" "))
            .map((part) -> part.substring(0, 1).toUpperCase() + part.substring(1).toLowerCase())
            .collect( Collectors.joining( " " ) );
    }

    public void setTimestamp(String date) throws ParseException {
        Date parsedDate = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss").parse(date);
        this.timestamp = new Timestamp(parsedDate.getTime());
    }

    public void setFahrenheit(String fahrenheit) {
        this.fahrenheit = Double.parseDouble(fahrenheit);
    }

    public void setCelsius(String celsius) {
        this.celsius = Double.parseDouble(celsius);
    }
}
