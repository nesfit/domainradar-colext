package cz.vut.fit.domainradar.models;

public final class ResultCodes {
    /**
     * The operation was successful.
     */
    public static final int OK = 0;

    /**
     * A generic error not caused inside the collecting system.
     */
    public static final int OTHER_EXTERNAL_ERROR = 10;

    /**
     * A generic error caused inside the collecting system (e.g. invalid state).
     */
    public static final int INTERNAL_ERROR = 20;

    /**
     * Invalid domain name.
     */
    public static final int INVALID_DOMAIN_NAME = 60;

    /**
     * Object does not exist.
     */
    public static final int NOT_FOUND = 30;

    /**
     * Invalid format of remote source's response.
     */
    public static final int INVALID_FORMAT = 40;

    /**
     * Error fetching from remote source.
     */
    public static final int CANNOT_FETCH = 50;

    /**
     * We are rate limited at the remote source.
     */
    public static final int RATE_LIMITED = 51;

    /**
     * Unexpected DNS error.
     */
    public static final int OTHER_DNS_ERROR = 70;

    /**
     * RDAP servers are not available for the entity.
     */
    public static final int RDAP_NOT_AVAILABLE = 80;
}