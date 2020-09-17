package com.cosminsanda;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@NoArgsConstructor
public class TemperatureReading {

    @Getter @Setter private String xml = null;
    @Getter @Setter private Reading reading = null;

    public TemperatureReading(String xml, Reading reading) {
        this.xml = xml;
        this.reading = reading;
    }
}
