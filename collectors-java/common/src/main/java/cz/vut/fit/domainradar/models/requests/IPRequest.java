package cz.vut.fit.domainradar.models.requests;

import org.jetbrains.annotations.Nullable;

import java.util.List;

/**
 * A record representing a request for the IP-based collectors.
 *
 * @param collectors A list of collector identifiers of the collectors that should process this request.
 * @author Ondřej Ondryáš
 */
public record IPRequest(
        @Nullable
        List<String> collectors
) {
}
