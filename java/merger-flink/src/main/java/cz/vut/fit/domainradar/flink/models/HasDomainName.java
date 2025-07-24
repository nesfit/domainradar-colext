package cz.vut.fit.domainradar.flink.models;

import org.jetbrains.annotations.NotNull;

public interface HasDomainName {
    /**
     * Gets the associated domain name.
     *
     * @return The domain name as a non-null string.
     */
    @NotNull
    String getDomainName();
}
