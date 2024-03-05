package cz.vut.fit.domainradar.models.ip;

import org.jetbrains.annotations.NotNull;

public record RTTData(boolean available,
                      double rtt,
                      double dropped,
                      @NotNull String location) {
}
