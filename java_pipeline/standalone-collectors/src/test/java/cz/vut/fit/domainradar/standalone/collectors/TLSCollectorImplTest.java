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

        var result = tlsCollector.collect("www.su.fit.vutbr.cz", "147.229.177.160");
        assertNotNull(result);
        assertEquals(result.statusCode(), 0);
        assertNotNull(result.html());
        assertTrue(result.html().contains("radostnějším"));
    }

    @Test
    void limitRedirectsExact() {
        final var tlsCollector = new TLSCollectorImpl(1, 5000);

        var result = tlsCollector.collect("www.su.fit.vutbr.cz", "147.229.177.160");
        assertNotNull(result);
        assertEquals(result.statusCode(), 0);
        assertNotNull(result.html());
        assertTrue(result.html().contains("radostnějším"));
    }

    @Test
    void limitRedirects() {
        final var tlsCollector = new TLSCollectorImpl(0, 5000);

        var result = tlsCollector.collect("www.su.fit.vutbr.cz", "147.229.177.160");
        assertNotNull(result);
        assertNull(result.html());
    }

    @Test
    void limitTime() {
        final var tlsCollector = new TLSCollectorImpl(0, 5);

        var result = tlsCollector.collect("www.su.fit.vutbr.cz", "147.229.177.160");
        assertNotNull(result);
        assertEquals(result.statusCode(), ResultCodes.TIMEOUT);
    }
}