package com.cosminsanda;


import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;
import java.sql.Timestamp;

@NoArgsConstructor
public class Reading implements Serializable {
    @Getter @Setter private Timestamp timestamp = null;
    @Getter @Setter private String city = null;
    @Getter @Setter private Double celsius = null;
    @Getter @Setter private Double fahrenheit = null;

    public Reading(String city, Timestamp timestamp, Double fahrenheit, Double celsius) {
        this.city = city;
        this.timestamp = timestamp;
        if (fahrenheit == null && celsius == null) throw new IllegalArgumentException("One of fahrenheit or celsius must be set");
        if (fahrenheit != null) this.fahrenheit = fahrenheit;
        if (celsius != null) this.celsius = celsius;
    }
}
