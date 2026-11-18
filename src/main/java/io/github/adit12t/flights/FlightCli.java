package io.github.adit12t.flights;

import io.github.adit12t.flights.FlightFileParser.Rejection;
import io.github.adit12t.flights.FlightRepository.Search;
import io.github.adit12t.flights.FlightRepository.Sort;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.Clock;
import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Command line:
 * <pre>
 *   load FILE [--report FILE]      validate a file and store the valid rows
 *   list [--sort price|price-desc|date]
 *   search [--from CODE] [--to CODE] [--after yyyy-MM-dd] [--before yyyy-MM-dd] [--seats N] [--sort ...]
 * </pre>
 * Database settings come from FLIGHTS_DB_URL, FLIGHTS_DB_USER and FLIGHTS_DB_PASSWORD; the default is a
 * local H2 file. FLIGHTS_AIRLINES (comma separated) overrides the allowed airlines.
 */
public final class FlightCli {

    private final FlightRepository repository;
    private final FlightFileParser parser;
    private final PrintStream out;

    FlightCli(FlightRepository repository, FlightFileParser parser, PrintStream out) {
        this.repository = repository;
        this.parser = parser;
        this.out = out;
    }

    public static void main(String[] args) {
        Map<String, String> env = System.getenv();
        String url = env.getOrDefault("FLIGHTS_DB_URL", "jdbc:h2:./flights");
        String user = env.getOrDefault("FLIGHTS_DB_USER", "sa");
        String password = env.getOrDefault("FLIGHTS_DB_PASSWORD", "");
        Supplier<Connection> connections = () -> {
            try {
                return DriverManager.getConnection(url, user, password);
            } catch (SQLException e) {
                throw new IllegalStateException("Cannot connect to " + url + ": " + e.getMessage(), e);
            }
        };
        Set<String> airlines = env.containsKey("FLIGHTS_AIRLINES")
                ? Set.copyOf(Arrays.stream(env.get("FLIGHTS_AIRLINES").split(",")).map(String::strip).filter(s -> !s.isEmpty()).toList())
                : FlightFileParser.DEFAULT_AIRLINES;
        FlightCli cli = new FlightCli(new FlightRepository(connections), new FlightFileParser(airlines, Clock.systemDefaultZone()), System.out);
        try {
            System.exit(cli.run(args));
        } catch (IllegalArgumentException | IllegalStateException e) {
            System.err.println("Error: " + e.getMessage());
            System.err.println(usage());
            System.exit(2);
        } catch (IOException | SQLException e) {
            System.err.println("Error: " + e.getMessage());
            System.exit(1);
        }
    }

    /** Returns the process exit code: 0 on success, 3 if a load rejected some rows. */
    int run(String[] args) throws IOException, SQLException {
        if (args.length == 0) {
            throw new IllegalArgumentException("No command given");
        }
        repository.createSchema();
        Map<String, String> opts = options(Arrays.copyOfRange(args, 1, args.length), args[0].equals("load") ? 1 : 0);
        return switch (args[0]) {
            case "load" -> load(Path.of(opts.get("_0")), opts.get("report"));
            case "list" -> print(repository.find(Search.any(), sort(opts.get("sort"))));
            case "search" -> print(repository.find(new Search(opts.get("from"), opts.get("to"), date(opts.get("after")),
                    date(opts.get("before")), opts.containsKey("seats") ? number(opts.get("seats")) : null), sort(opts.get("sort"))));
            default -> throw new IllegalArgumentException("Unknown command '" + args[0] + "'");
        };
    }

    private int load(Path file, String reportFile) throws IOException, SQLException {
        FlightFileParser.Result result = parser.parse(Files.newBufferedReader(file));
        FlightRepository.LoadSummary summary = repository.insertAll(result.flights());
        out.printf("Read %d valid rows: %d stored, %d already stored. Rejected %d rows.%n",
                result.flights().size(), summary.inserted(), summary.skippedAsDuplicates(), result.rejections().size());
        for (Rejection r : result.rejections()) {
            out.println("  " + r);
        }
        if (reportFile != null) {
            List<String> lines = result.rejections().stream().map(r -> r.lineNumber() + "\t" + r.reason() + "\t" + r.line()).toList();
            Files.write(Path.of(reportFile), lines);
            out.println("Rejection report written to " + reportFile);
        }
        return result.rejections().isEmpty() ? 0 : 3;
    }

    private int print(List<Flight> flights) {
        if (flights.isEmpty()) {
            out.println("No flights found.");
            return 0;
        }
        out.printf("%-10s %-22s %-22s %-10s %10s %6s%n", "Airline", "From", "To", "Date", "Price", "Seats");
        for (Flight f : flights) {
            out.printf("%-10s %-22s %-22s %-10s %10s %6d%n", f.airline(), f.originCity() + " (" + f.originCode() + ")",
                    f.destinationCity() + " (" + f.destinationCode() + ")", f.departureDate(), "$" + f.price(), f.seatsAvailable());
        }
        return 0;
    }

    private static Map<String, String> options(String[] args, int positional) {
        Map<String, String> opts = new HashMap<>();
        int pos = 0;
        for (int i = 0; i < args.length; i++) {
            if (args[i].startsWith("--")) {
                if (i + 1 >= args.length) {
                    throw new IllegalArgumentException("Missing value for " + args[i]);
                }
                opts.put(args[i].substring(2), args[++i]);
            } else {
                opts.put("_" + pos++, args[i]);
            }
        }
        if (pos != positional) {
            throw new IllegalArgumentException("Expected " + positional + " file argument(s), found " + pos);
        }
        return opts;
    }

    private static Sort sort(String value) {
        if (value == null) {
            return Sort.PRICE_ASC;
        }
        return switch (value.toLowerCase(Locale.ROOT)) {
            case "price" -> Sort.PRICE_ASC;
            case "price-desc" -> Sort.PRICE_DESC;
            case "date" -> Sort.DATE;
            default -> throw new IllegalArgumentException("Unknown sort '" + value + "'");
        };
    }

    private static LocalDate date(String value) {
        try {
            return value == null ? null : LocalDate.parse(value);
        } catch (DateTimeParseException e) {
            throw new IllegalArgumentException("Dates must be yyyy-MM-dd: " + value);
        }
    }

    private static int number(String value) {
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Not a whole number: " + value);
        }
    }

    static String usage() {
        return """
                Usage:
                  load FILE [--report FILE]
                  list [--sort price|price-desc|date]
                  search [--from CODE] [--to CODE] [--after yyyy-MM-dd] [--before yyyy-MM-dd] [--seats N] [--sort ...]""";
    }
}
