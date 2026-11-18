package io.github.adit12t.flights;

import io.github.adit12t.flights.FlightRepository.Search;
import io.github.adit12t.flights.FlightRepository.Sort;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Clock;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FlightRepositoryTest {

    private FlightRepository repository;

    private static Flight flight(String airline, String from, String to, String date, String price, int seats) {
        return new Flight(airline, "City " + from, "State", from, "City " + to, "State", to, LocalDate.parse(date), new BigDecimal(price), seats);
    }

    @BeforeEach
    void setUp() throws SQLException {
        String url = "jdbc:h2:mem:" + UUID.randomUUID() + ";DB_CLOSE_DELAY=-1";
        repository = new FlightRepository(() -> {
            try {
                return DriverManager.getConnection(url, "sa", "");
            } catch (SQLException e) {
                throw new IllegalStateException(e);
            }
        });
        repository.createSchema();
        repository.createSchema();   // safe to run twice
    }

    @Test
    void storesFlightsAndSkipsOnesAlreadyThere() throws SQLException {
        var a = flight("Delta", "DTW", "BNA", "2027-03-10", "310.58", 10);
        var b = flight("United", "CLT", "AUS", "2027-03-31", "360.58", 32);
        assertEquals(new FlightRepository.LoadSummary(2, 0), repository.insertAll(List.of(a, b)));
        assertEquals(new FlightRepository.LoadSummary(0, 1), repository.insertAll(List.of(a)));
        assertEquals(List.of(a, b), repository.find(Search.any(), Sort.PRICE_ASC));
        assertEquals(List.of(b, a), repository.find(Search.any(), Sort.PRICE_DESC));
    }

    @Test
    void searchesByRouteDatesAndSeats() throws SQLException {
        var cheap = flight("Delta", "DTW", "BNA", "2027-03-10", "120.00", 0);
        var later = flight("United", "DTW", "BNA", "2027-04-01", "200.00", 5);
        var other = flight("American", "CAE", "JFK", "2027-03-15", "150.00", 9);
        repository.insertAll(List.of(cheap, later, other));

        assertEquals(List.of(cheap, later), repository.find(new Search("dtw", "BNA", null, null, null), Sort.PRICE_ASC));
        assertEquals(List.of(later), repository.find(new Search("DTW", null, null, null, 1), Sort.PRICE_ASC));
        assertEquals(List.of(cheap, other), repository.find(
                new Search(null, null, LocalDate.parse("2027-03-01"), LocalDate.parse("2027-03-31"), null), Sort.DATE));
    }

    @Test
    void searchValuesAreBoundNotConcatenated() throws SQLException {
        repository.insertAll(List.of(flight("Delta", "DTW", "BNA", "2027-03-10", "120.00", 3)));
        assertEquals(List.of(), repository.find(new Search("DTW' OR '1'='1", null, null, null, null), Sort.PRICE_ASC));
    }

    @Test
    void cliLoadsTheSampleFileAndReportsRejections() throws Exception {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        Clock clock = Clock.fixed(Instant.parse("2027-01-15T12:00:00Z"), ZoneOffset.UTC);
        FlightCli cli = new FlightCli(repository, new FlightFileParser(FlightFileParser.DEFAULT_AIRLINES, clock), new PrintStream(buffer, true));
        Path report = Files.createTempFile("rejected", ".tsv");

        assertEquals(3, cli.run(new String[]{"load", "data/flights.txt", "--report", report.toString()}));
        String output = buffer.toString();
        assertTrue(output.startsWith("Read 4 valid rows: 4 stored, 0 already stored. Rejected 4 rows."), output);
        assertTrue(output.contains("line 3: airline not allowed: ABC"), output);
        assertEquals(4, Files.readAllLines(report).size());

        buffer.reset();
        assertEquals(0, cli.run(new String[]{"search", "--from", "DTW", "--to", "BNA"}));
        assertTrue(buffer.toString().contains("Detroit") || buffer.toString().contains("DTW"), buffer.toString());
        Files.delete(report);
    }
}
