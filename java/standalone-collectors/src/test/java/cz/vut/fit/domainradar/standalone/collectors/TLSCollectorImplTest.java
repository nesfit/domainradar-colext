package cz.vut.fit.domainradar.standalone.collectors;

import cz.vut.fit.domainradar.models.ResultCodes;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Disclaimer: I know these are not great unit tests, depending on a single remote service and everything...
 * I just needed to do at least *some* testing.
 */
class TLSCollectorImplTest {

    @Test
    void collect() {
        final var tlsCollector = new TLSCollectorImpl(10, 5000);

        var result = tlsCollector.collect("www.fit.vut.cz", "147.229.9.65");
        assertNotNull(result);
        assertEquals(result.statusCode(), 0);
        assertNotNull(result.html());
        assertTrue(result.html().contains("<html"));
    }

    @Test
    void collectWithRedirects() {
        final var tlsCollector = new TLSCollectorImpl(10, 5000);

        var result = tlsCollector.collect("fit.vut.cz", "147.229.9.65");
        assertNotNull(result);
        assertEquals(result.statusCode(), 0);
        assertNotNull(result.html());
        assertTrue(result.html().contains("<html"));
    }

    @Test
    void limitRedirectsExact() {
        final var tlsCollector = new TLSCollectorImpl(1, 5000);

        var result = tlsCollector.collect("fit.vut.cz", "147.229.9.65");
        assertNotNull(result);
        assertEquals(result.statusCode(), 0);
        assertNotNull(result.html());
        assertTrue(result.html().contains("<html"));
    }

    @Test
    void limitRedirects() {
        final var tlsCollector = new TLSCollectorImpl(0, 5000);

        var result = tlsCollector.collect("fit.vut.cz", "147.229.9.65");
        assertNotNull(result);
        assertNull(result.html());
    }

    @Test
    void limitTime() {
        final var tlsCollector = new TLSCollectorImpl(0, 5);

        var result = tlsCollector.collect("fit.vut.cz", "147.229.9.65");
        assertNotNull(result);
        assertEquals(result.statusCode(), ResultCodes.TIMEOUT);
    }
}