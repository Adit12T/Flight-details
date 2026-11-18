package io.github.adit12t.flights;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.math.BigDecimal;
import java.time.Clock;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.format.ResolverStyle;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Reads pipe-delimited flight rows:
 * {@code Airline|Origin|Origin state|Origin code|Destination|Destination state|Destination code|MM/dd/yyyy|Price|Seats}.
 * Every row is checked and every problem is reported with its line number; one bad row never stops the file.
 */
public final class FlightFileParser {

    public record Rejection(int lineNumber, String reason, String line) {
        @Override
        public String toString() {
            return "line " + lineNumber + ": " + reason;
        }
    }

    public record Result(List<Flight> flights, List<Rejection> rejections) {
    }

    public static final Set<String> DEFAULT_AIRLINES = Set.of("Delta", "United", "Frontier", "American");

    private static final DateTimeFormatter DATE = DateTimeFormatter.ofPattern("MM/dd/uuuu")
            .withResolverStyle(ResolverStyle.STRICT);
    private static final Pattern IATA = Pattern.compile("[A-Z]{3}");
    private static final BigDecimal MAX_PRICE = new BigDecimal("100000");

    private final Set<String> allowedAirlines;
    private final Clock clock;

    public FlightFileParser(Set<String> allowedAirlines, Clock clock) {
        if (allowedAirlines.isEmpty()) {
            throw new IllegalArgumentException("At least one airline must be allowed");
        }
        this.allowedAirlines = allowedAirlines.stream().map(a -> a.toLowerCase(Locale.ROOT)).collect(Collectors.toUnmodifiableSet());
        this.clock = clock;
    }

    public Result parse(Reader input) throws IOException {
        List<Flight> flights = new ArrayList<>();
        List<Rejection> rejections = new ArrayList<>();
        Set<String> seen = new HashSet<>();
        LocalDate today = LocalDate.now(clock);
        try (BufferedReader reader = new BufferedReader(input)) {
            String line;
            int number = 0;
            while ((line = reader.readLine()) != null) {
                number++;
                if (line.isBlank() || line.startsWith("#")) {
                    continue;
                }
                try {
                    Flight flight = parseLine(line, today);
                    if (!seen.add(flight.key())) {
                        throw new IllegalArgumentException("duplicate of an earlier row");
                    }
                    flights.add(flight);
                } catch (IllegalArgumentException e) {
                    rejections.add(new Rejection(number, e.getMessage(), line));
                }
            }
        }
        return new Result(List.copyOf(flights), List.copyOf(rejections));
    }

    private Flight parseLine(String line, LocalDate today) {
        String[] f = line.split("\\|", -1);
        if (f.length != 10) {
            throw new IllegalArgumentException("expected 10 fields, found " + f.length);
        }
        for (int i = 0; i < f.length; i++) {
            f[i] = f[i].strip();
            if (f[i].isEmpty()) {
                throw new IllegalArgumentException("field " + (i + 1) + " is empty");
            }
        }
        String airline = f[0];
        if (!allowedAirlines.contains(airline.toLowerCase(Locale.ROOT))) {
            throw new IllegalArgumentException("airline not allowed: " + airline);
        }
        String originCode = code(f[3], "origin");
        String destinationCode = code(f[6], "destination");
        if (originCode.equals(destinationCode)) {
            throw new IllegalArgumentException("origin and destination are the same airport");
        }
        LocalDate date;
        try {
            date = LocalDate.parse(f[7], DATE);
        } catch (DateTimeParseException e) {
            throw new IllegalArgumentException("invalid date (use MM/dd/yyyy): " + f[7]);
        }
        if (date.isBefore(today)) {
            throw new IllegalArgumentException("date is in the past: " + f[7]);
        }
        BigDecimal price;
        try {
            price = new BigDecimal(f[8]);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("invalid price: " + f[8]);
        }
        if (price.signum() <= 0 || price.compareTo(MAX_PRICE) > 0 || price.scale() > 2) {
            throw new IllegalArgumentException("price must be above 0, at most 100000 and have at most 2 decimals: " + f[8]);
        }
        int seats;
        try {
            seats = Integer.parseInt(f[9]);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("invalid seat count: " + f[9]);
        }
        if (seats < 0 || seats > 1000) {
            throw new IllegalArgumentException("seat count must be from 0 to 1000: " + f[9]);
        }
        return new Flight(airline, f[1], f[2], originCode, f[4], f[5], destinationCode, date, price, seats);
    }

    private static String code(String value, String what) {
        String code = value.toUpperCase(Locale.ROOT);
        if (!IATA.matcher(code).matches()) {
            throw new IllegalArgumentException(what + " airport code must be 3 letters: " + value);
        }
        return code;
    }
}
