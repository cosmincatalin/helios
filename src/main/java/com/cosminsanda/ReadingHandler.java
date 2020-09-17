package com.cosminsanda;

import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Locale;
import java.util.stream.Collectors;

public class ReadingHandler extends DefaultHandler {

    private Reading reading;
    private String elementValue;
    private Boolean readingTemperatures = false;
    private String unit;

    private final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'hh:mm:ss");

    @Override
    public void characters(char[] ch, int start, int length) {
        elementValue = new String(ch, start, length);
    }

    @Override
    public void startDocument() {
        reading = new Reading();
    }

    @Override
    public void startElement(String uri, String lName, String qName, Attributes attr) throws SAXException {
        switch (qName) {
            case "temperature":
                readingTemperatures = true;
                break;
            case "value":
                if (readingTemperatures) {
                    unit = attr.getValue("unit");
                    if (!unit.equals("celsius") && !unit.equals("fahrenheit")) {
                        throw new SAXException("Only celsius and fahrenheit are accepted units for temperature.");
                    }
                }
                break;
        }
    }

    @Override
    public void endElement(String uri, String localName, String qName) throws SAXException {
        switch (qName) {
            case "city":
                reading.setCity(
                    Arrays.stream(elementValue.split(" "))
                        .map((part) -> part.substring(0, 1).toUpperCase() + part.substring(1).toLowerCase())
                        .collect( Collectors.joining( " " ) )
                );
                break;
            case "temperature":
                readingTemperatures = false;
                break;
            case "measured_at_ts":
                try {
                    Date parsedDate = dateFormat.parse(elementValue);
                    reading.setTimestamp(new Timestamp(parsedDate.getTime()));
                } catch (ParseException ex) {
                    throw new SAXException(ex);
                }
                break;
            case "value":
                if (readingTemperatures && unit.equals("celsius")) {
                    try {
                        reading.setCelsius(Double.parseDouble(elementValue));
                    } catch (Exception ex) {
                        throw new SAXException(ex);
                    }

                }
                if (readingTemperatures && unit.equals("fahrenheit")) {
                    try {
                        reading.setFahrenheit(Double.parseDouble(elementValue));
                    } catch (Exception ex) {
                        throw new SAXException(ex);
                    }
                }
                break;
        }
    }

    @Override
    public void endDocument() throws SAXException {
        if (reading.getCity() == null) {
            throw new SAXException("City must be provided.");
        }
        if (reading.getTimestamp() == null) {
            throw new SAXException("Timestamp must be provided.");
        }
        if (reading.getCelsius() == null && reading.getFahrenheit() == null) {
            throw new SAXException("At least one of Celsius and Fahrenheit must be provided.");
        }
        if (reading.getCelsius() != null && reading.getFahrenheit() != null &&
            (reading.getFahrenheit() < reading.getCelsius() * 1.8 + 31 ||
            reading.getFahrenheit() > reading.getCelsius() * 1.8 + 33 )) {
            throw new SAXException("Celsius and Fahrenheit readings must correspond.");
        }

        if (reading.getCelsius() != null && reading.getFahrenheit() == null) {
            reading.setFahrenheit(Double.parseDouble(String.format(Locale.US, "%.2f", reading.getCelsius() * 1.8 + 32)));
        }
        if (reading.getCelsius() == null && reading.getFahrenheit() != null) {
            reading.setCelsius(Double.parseDouble(String.format(Locale.US, "%.2f", (reading.getFahrenheit() - 32) / 1.8)));
        }
    }

    public Reading getReading() {
        return reading;
    }
}
