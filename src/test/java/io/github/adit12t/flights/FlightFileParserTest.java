package io.github.adit12t.flights;

import org.junit.jupiter.api.Test;

import java.io.StringReader;
import java.math.BigDecimal;
import java.time.Clock;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class FlightFileParserTest {

    private static final Clock CLOCK = Clock.fixed(Instant.parse("2027-01-15T12:00:00Z"), ZoneOffset.UTC);
    private final FlightFileParser parser = new FlightFileParser(FlightFileParser.DEFAULT_AIRLINES, CLOCK);

    private FlightFileParser.Result parse(String text) throws Exception {
        return parser.parse(new StringReader(text));
    }

    private String reasonFor(String line) throws Exception {
        List<FlightFileParser.Rejection> r = parse(line).rejections();
        assertEquals(1, r.size(), "expected one rejection for " + line);
        return r.get(0).reason();
    }

    @Test
    void parsesAValidRow() throws Exception {
        var result = parse("delta|Detroit|Michigan|dtw|Nashville|Tennessee|BNA|03/10/2027|310.58|10\n");
        assertEquals(List.of(), result.rejections());
        Flight f = result.flights().get(0);
        assertEquals("DTW", f.originCode());
        assertEquals(LocalDate.of(2027, 3, 10), f.departureDate());
        assertEquals(new BigDecimal("310.58"), f.price());
        assertEquals(10, f.seatsAvailable());
    }

    @Test
    void reportsEachProblem() throws Exception {
        assertEquals("airline not allowed: ABC", reasonFor("ABC|A|B|DTW|C|D|BNA|03/10/2027|1|1"));
        assertEquals("date is in the past: 01/14/2027", reasonFor("Delta|A|B|DTW|C|D|BNA|01/14/2027|1|1"));
        assertEquals("invalid date (use MM/dd/yyyy): 02/30/2027", reasonFor("Delta|A|B|DTW|C|D|BNA|02/30/2027|1|1"));
        assertEquals("origin airport code must be 3 letters: DT", reasonFor("Delta|A|B|DT|C|D|BNA|03/10/2027|1|1"));
        assertEquals("origin and destination are the same airport", reasonFor("Delta|A|B|ORD|C|D|ORD|03/10/2027|1|1"));
        assertEquals("invalid price: abc", reasonFor("Delta|A|B|DTW|C|D|BNA|03/10/2027|abc|1"));
        assertEquals("price must be above 0, at most 100000 and have at most 2 decimals: 1.234",
                reasonFor("Delta|A|B|DTW|C|D|BNA|03/10/2027|1.234|1"));
        assertEquals("seat count must be from 0 to 1000: -1", reasonFor("Delta|A|B|DTW|C|D|BNA|03/10/2027|1|-1"));
        assertEquals("expected 10 fields, found 3", reasonFor("Delta|A|B"));
        assertEquals("field 2 is empty", reasonFor("Delta| |B|DTW|C|D|BNA|03/10/2027|1|1"));
    }

    @Test
    void todayIsAllowedAndDuplicatesAreRejected() throws Exception {
        var result = parse("""
                Delta|A|B|DTW|C|D|BNA|01/15/2027|1|1
                DELTA|A|B|DTW|C|D|BNA|01/15/2027|2|2
                """);
        assertEquals(1, result.flights().size());
        assertEquals(2, result.rejections().get(0).lineNumber());
        assertEquals("duplicate of an earlier row", result.rejections().get(0).reason());
    }

    @Test
    void skipsCommentsAndBlankLinesButKeepsLineNumbers() throws Exception {
        var result = parse("# header\n\nABC|A|B|DTW|C|D|BNA|03/10/2027|1|1\n");
        assertEquals(3, result.rejections().get(0).lineNumber());
    }

    @Test
    void sampleFileHasFourGoodRowsAndFourBad() throws Exception {
        var result = parser.parse(java.nio.file.Files.newBufferedReader(java.nio.file.Path.of("data/flights.txt")));
        assertEquals(4, result.flights().size());
        assertEquals(4, result.rejections().size());
    }

    @Test
    void airlineListMustNotBeEmpty() {
        assertThrows(IllegalArgumentException.class, () -> new FlightFileParser(Set.of(), CLOCK));
    }
}
