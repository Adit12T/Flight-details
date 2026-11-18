package io.github.adit12t.flights;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.Objects;

/** One scheduled flight. Airport codes are three-letter IATA codes in upper case. */
public record Flight(String airline,
                     String originCity, String originState, String originCode,
                     String destinationCity, String destinationState, String destinationCode,
                     LocalDate departureDate, BigDecimal price, int seatsAvailable) {

    public Flight {
        Objects.requireNonNull(airline, "airline");
        Objects.requireNonNull(departureDate, "departureDate");
        Objects.requireNonNull(price, "price");
    }

    /** What makes two rows the same flight: airline, route and date. */
    public String key() {
        return airline.toLowerCase() + "|" + originCode + "|" + destinationCode + "|" + departureDate;
    }
}
