package io.github.adit12t.flights;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

/** Stores and queries flights with plain JDBC. Every value goes through a bound parameter. */
public final class FlightRepository {

    public enum Sort {
        PRICE_ASC("price ASC"), PRICE_DESC("price DESC"), DATE("departure_date ASC, price ASC");

        private final String orderBy;

        Sort(String orderBy) {
            this.orderBy = orderBy;
        }
    }

    /** Optional filters; null means "any". */
    public record Search(String originCode, String destinationCode, LocalDate from, LocalDate to, Integer minSeats) {
        public static Search any() {
            return new Search(null, null, null, null, null);
        }
    }

    public record LoadSummary(int inserted, int skippedAsDuplicates) {
    }

    private final Supplier<Connection> connections;

    public FlightRepository(Supplier<Connection> connections) {
        this.connections = connections;
    }

    public void createSchema() throws SQLException {
        String sql;
        try (InputStream in = FlightRepository.class.getResourceAsStream("/schema.sql")) {
            sql = new String(in.readAllBytes(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        try (Connection c = connections.get(); Statement s = c.createStatement()) {
            for (String part : sql.split(";")) {
                if (!part.isBlank()) {
                    s.execute(part);
                }
            }
        }
    }

    /** Inserts all flights in one transaction, skipping any already stored. */
    public LoadSummary insertAll(List<Flight> flights) throws SQLException {
        String exists = "SELECT 1 FROM flight WHERE LOWER(airline) = ? AND origin_code = ? AND destination_code = ? AND departure_date = ?";
        String insert = """
                INSERT INTO flight (airline, origin_city, origin_state, origin_code, destination_city, destination_state,
                                    destination_code, departure_date, price, seats_available)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""";
        try (Connection c = connections.get()) {
            c.setAutoCommit(false);
            int inserted = 0, skipped = 0;
            try (PreparedStatement check = c.prepareStatement(exists); PreparedStatement ins = c.prepareStatement(insert)) {
                for (Flight f : flights) {
                    check.setString(1, f.airline().toLowerCase());
                    check.setString(2, f.originCode());
                    check.setString(3, f.destinationCode());
                    check.setDate(4, Date.valueOf(f.departureDate()));
                    try (ResultSet rs = check.executeQuery()) {
                        if (rs.next()) {
                            skipped++;
                            continue;
                        }
                    }
                    ins.setString(1, f.airline());
                    ins.setString(2, f.originCity());
                    ins.setString(3, f.originState());
                    ins.setString(4, f.originCode());
                    ins.setString(5, f.destinationCity());
                    ins.setString(6, f.destinationState());
                    ins.setString(7, f.destinationCode());
                    ins.setDate(8, Date.valueOf(f.departureDate()));
                    ins.setBigDecimal(9, f.price());
                    ins.setInt(10, f.seatsAvailable());
                    ins.addBatch();
                    inserted++;
                }
                ins.executeBatch();
                c.commit();
            } catch (SQLException e) {
                c.rollback();
                throw e;
            }
            return new LoadSummary(inserted, skipped);
        }
    }

    public List<Flight> find(Search search, Sort sort) throws SQLException {
        StringBuilder sql = new StringBuilder("SELECT * FROM flight WHERE 1 = 1");
        List<Object> params = new ArrayList<>();
        Optional.ofNullable(search.originCode()).ifPresent(v -> { sql.append(" AND origin_code = ?"); params.add(v.toUpperCase()); });
        Optional.ofNullable(search.destinationCode()).ifPresent(v -> { sql.append(" AND destination_code = ?"); params.add(v.toUpperCase()); });
        Optional.ofNullable(search.from()).ifPresent(v -> { sql.append(" AND departure_date >= ?"); params.add(Date.valueOf(v)); });
        Optional.ofNullable(search.to()).ifPresent(v -> { sql.append(" AND departure_date <= ?"); params.add(Date.valueOf(v)); });
        Optional.ofNullable(search.minSeats()).ifPresent(v -> { sql.append(" AND seats_available >= ?"); params.add(v); });
        sql.append(" ORDER BY ").append(sort.orderBy);   // from the enum, never from user input
        try (Connection c = connections.get(); PreparedStatement ps = c.prepareStatement(sql.toString())) {
            for (int i = 0; i < params.size(); i++) {
                ps.setObject(i + 1, params.get(i));
            }
            List<Flight> out = new ArrayList<>();
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    out.add(new Flight(rs.getString("airline"), rs.getString("origin_city"), rs.getString("origin_state"),
                            rs.getString("origin_code").strip(), rs.getString("destination_city"), rs.getString("destination_state"),
                            rs.getString("destination_code").strip(), rs.getDate("departure_date").toLocalDate(),
                            rs.getBigDecimal("price"), rs.getInt("seats_available")));
                }
            }
            return out;
        }
    }
}
