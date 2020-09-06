package com.cosminsanda;


import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.stream.Collectors;

public class Reading {
    private Timestamp timestamp = null;
    private String city = null;
    private Double celsius = null;
    private Double fahrenheit = null;

    public Reading() {

    }

    public String getCity() {
        return city;
    }

    public void setCity(String city) {
        this.city = Arrays.stream(city.split(" "))
            .map((part) -> part.substring(0, 1).toUpperCase() + part.substring(1).toLowerCase())
            .collect( Collectors.joining( " " ) );
    }

    public Timestamp getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String date) throws ParseException {
        Date parsedDate = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss").parse(date);
        this.timestamp = new Timestamp(parsedDate.getTime());
    }

    public void setFahrenheit(String fahrenheit) {
        this.fahrenheit = Double.parseDouble(fahrenheit);
    }

    public Double getFahrenheit() {
        return fahrenheit;
    }

    public void setCelsius(String celsius) {
        this.celsius = Double.parseDouble(celsius);
    }

    public Double getCelsius() {
        return celsius;
    }
}
