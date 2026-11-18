# Flight Loader

[![CI](https://github.com/Adit12T/Flight-details/actions/workflows/ci.yml/badge.svg)](https://github.com/Adit12T/Flight-details/actions/workflows/ci.yml)

A **Java 21** command-line tool that validates a flight schedule file, loads the good rows into a SQL
database (H2 by default, or MySQL) and searches them by route, date and seats.

## Usage

```bash
mvn -q package
mvn -q exec:java -Dexec.args="load data/flights.txt --report rejected.tsv"
```

```
Read 4 valid rows: 4 stored, 0 already stored. Rejected 4 rows.
  line 3: airline not allowed: ABC
  line 5: date is in the past: 11/12/2018
  line 7: origin and destination are the same airport
  line 9: invalid date (use MM/dd/yyyy): 02/30/2027
Rejection report written to rejected.tsv
```

```bash
mvn -q exec:java -Dexec.args="list --sort price-desc"
mvn -q exec:java -Dexec.args="search --from DTW --to BNA --after 2027-03-01 --seats 1"
```

`load` exits with code 3 when some rows were rejected, so scripts can notice.

## Input format

One flight per line, pipe-separated; blank lines and lines starting with `#` are skipped:

```
Airline|Origin|Origin state|Origin code|Destination|Destination state|Destination code|MM/dd/yyyy|Price|Seats
Delta|Detroit|Michigan|DTW|Nashville|Tennessee|BNA|03/10/2027|310.58|10
```

Every row is checked and every problem is reported with its line number; one bad row never stops
the load. A row is rejected when:

- the airline isn't on the allowed list (Delta, United, Frontier and American by default);
- an airport code isn't three letters, or origin and destination are the same;
- the date isn't a real `MM/dd/yyyy` date, or is in the past;
- the price isn't above 0 with at most two decimals, or the seat count isn't 0–1000;
- a field is missing or empty, or the row repeats an earlier one (same airline, route and date).

Rows already in the database are skipped rather than stored twice, and each load runs in a single
transaction.

## Configuration

| Variable | Default | Purpose |
|---|---|---|
| `FLIGHTS_DB_URL` | `jdbc:h2:./flights` | JDBC URL, e.g. `jdbc:mysql://localhost:3306/flights` |
| `FLIGHTS_DB_USER` | `sa` | Database user |
| `FLIGHTS_DB_PASSWORD` | *(empty)* | Database password |
| `FLIGHTS_AIRLINES` | `Delta,United,Frontier,American` | Allowed airlines, comma-separated |

The table is created on first run from [`schema.sql`](src/main/resources/schema.sql), which works on
H2 and MySQL 8: prices are `DECIMAL(10,2)`, and the database itself rejects non-positive prices,
negative seat counts and duplicate flights.

## What changed from the original

The 2018 version was a single Eclipse class that connected to MySQL as `root` with the password in
the source, dropped and recreated the table on every run, stored prices as `FLOAT`, redirected
`System.err` to a file to log problems, and only checked the airline and date. Version 2 takes its
credentials from the environment, keeps existing data, validates every field, reports rejections with
line numbers, adds search, and has tests for the parser, the repository (on in-memory H2) and the
command line.

## Tests

```bash
mvn verify
```

## License

Copyright © 2026 Aditya Tripathi. All rights reserved. The code is published for viewing only; see
[LICENSE](LICENSE).
