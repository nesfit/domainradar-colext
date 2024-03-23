package cz.vut.fit.domainradar.standalone.collectors;

import com.google.common.net.InternetDomainName;
import cz.vut.fit.domainradar.models.ResultCodes;
import cz.vut.fit.domainradar.models.dns.DNSData;
import cz.vut.fit.domainradar.models.dns.ZoneInfo;
import cz.vut.fit.domainradar.models.results.ZoneResult;
import org.slf4j.LoggerFactory;
import org.xbill.DNS.*;
import org.xbill.DNS.lookup.LookupSession;

import java.net.InetAddress;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class InternalDNSResolver {
    private static final org.slf4j.Logger Logger = LoggerFactory.getLogger(InternalDNSResolver.class);

    private record ZoneLookupResult(Name name, SOARecord soa, Throwable exception) {
        public ZoneLookupResult(Name name, SOARecord soa) {
            this(name, soa, null);
        }
    }

    private final Resolver _mainResolver;
    private final ExecutorService _executor;

    public InternalDNSResolver(Resolver resolver, ExecutorService executor) {
        _mainResolver = resolver;
        _executor = executor;
    }

    public CompletionStage<ZoneResult> getZoneInfo(String domainName) {
        // Make a LookupSession using the resolver's executor
        final var lookupSession = getLookupSession();

        // Parse the domain name using Guava's InternetDomainName
        InternetDomainName guavaName;
        try {
            guavaName = InternetDomainName.from(domainName);
        } catch (IllegalArgumentException e) {
            Logger.info("Invalid domain name: {}", domainName, e);
            return errorResultStage(ResultCodes.INVALID_DOMAIN_NAME, "Invalid domain name (generic parsing error)");
        }

        // Check if the domain name has a public suffix (TLD or "eTLD")
        if (!guavaName.hasPublicSuffix()) {
            Logger.debug("Domain name does not have a valid public suffix: {}", domainName);
            return errorResultStage(ResultCodes.INVALID_DOMAIN_NAME, "Invalid domain name (no public suffix)");
        }

        Name baseName = Name.root;
        var components = guavaName.parts();
        if (guavaName.isPublicSuffix()) {
            // Resolve SOA for the whole suffix.
            // This passes "." as the base name; zoneLookupStep will begin by merging it with the rightmost
            // component (e.g. "jp" in "hokkaido.jp").
            var lookupResult = findTopmostZoneSOA(baseName, components, components.size() - 1,
                    lookupSession, null, false);

            //
            return resolveIpsForZone(lookupResult, guavaName);
        } else if (guavaName.registrySuffix() != null) {
            // Start at the name
            try {
                var name = guavaName.topDomainUnderRegistrySuffix();
                baseName = Name.fromString(name.toString());
                components = components.subList(0, components.size() - name.parts().size());

                var lookupResult = findTopmostZoneSOA(baseName, components, components.size(),
                        lookupSession, null, true);
                return resolveIpsForZone(lookupResult, guavaName);
            } catch (IllegalStateException e) {
                // Shouldn't happen: either the domain is a public suffix (handled above),
                // or it is invalid (then hasPublicSuffix() is false), or it is at least a registry suffix
                // (which is also a public suffix) + something on top of it
                return errorResultStage(ResultCodes.OTHER_EXTERNAL_ERROR, "Invalid collector state");
            } catch (TextParseException e) {
                // Shouldn't happen
                Logger.warn("Invalid state (guava name part is invalid dnsjava name): {}", guavaName, e);
                return errorResultStage(ResultCodes.OTHER_EXTERNAL_ERROR, "Invalid collector state");
            }
        }

        return errorResultStage(ResultCodes.OTHER_EXTERNAL_ERROR, "Unknown error");
    }

    private CompletionStage<ZoneLookupResult> findTopmostZoneSOA(
            Name baseName, List<String> componentsToTry, int componentIndex,
            LookupSession session, ZoneLookupResult previousResult, boolean firstStep) {

        // Terminating condition
        if (componentIndex < 0 && !firstStep) {
            return CompletableFuture.completedFuture(previousResult);
        }

        Name name;
        if (firstStep) {
            // This is used when the lookup starts with the top domain under registry suffix
            // to make it more efficient (this way we don't have to merge the suffix with the first component)
            name = baseName;
        } else {
            // This is used in the other steps or when starting from the root name "."
            // Adds the current component as a prefix of the base name
            try {
                name = Name.fromString(componentsToTry.get(componentIndex), baseName);
            } catch (TextParseException e) {
                // Shouldn't happen
                Logger.warn("Invalid name in a lookup step: {}", componentsToTry.get(componentIndex));
                return CompletableFuture.completedFuture(previousResult);
            }
        }

        return session.lookupAsync(name, Type.SOA)
                .handleAsync((result, exc) -> {
                    if (exc != null) {
                        // An exception occurred when performing the DNS lookup
                        exc = exc.getCause();
                        // NoSuchDomain is fine, means it just doesn't exist
                        if (exc instanceof org.xbill.DNS.lookup.NoSuchDomainException) {
                            return CompletableFuture.completedFuture(previousResult);
                        }
                        // Others should be at least logged
                        Logger.debug("SOA lookup failed for {}", name, exc);
                        // If no result was carried over from the previous step, return
                        // a result with the error which is propagated to the final result
                        if (previousResult == null) {
                            return CompletableFuture.completedFuture(
                                    new ZoneLookupResult(null, null, exc));
                        } else {
                            // If a result was carried over (i.e. more general domain name
                            // has a SOA), return that
                            return CompletableFuture.completedFuture(previousResult);
                        }
                    } else {
                        // The lookup was successful
                        if (result == null) {
                            // Shouldn't happen
                            return CompletableFuture.completedFuture(previousResult);
                        }

                        var records = result.getRecords();
                        if (records.isEmpty()) {
                            // No SOA found, return the previous good result
                            return CompletableFuture.completedFuture(previousResult);
                        } else {
                            // Found a SOA - make an intermediary result, call ourselves again to try
                            // with one more component added to the left.
                            // The inner call will return our successful intermediary result if it fails.
                            var lookupResult = new ZoneLookupResult(name, (SOARecord) records.getFirst());
                            return findTopmostZoneSOA(name, componentsToTry, componentIndex - 1,
                                    session, lookupResult, false);
                        }
                    }
                }, _executor)
                // extract the CompletionStage created inside the handleAsync call
                .thenCompose(Function.identity());
    }

    private CompletionStage<ZoneResult> resolveIpsForZone(CompletionStage<ZoneLookupResult> lookupResultStage,
                                                          InternetDomainName name) {
        final var publicSuffix = name.publicSuffix() != null ? name.publicSuffix().toString() : "";
        final var registrySuffix = name.registrySuffix() != null ? name.registrySuffix().toString() : "";

        return lookupResultStage.exceptionally(e -> {
                    Logger.debug("Zone/SOA not found record for {}", name, e);
                    return null;
                })
                .thenCompose(result -> {
                    if (result != null && result.exception != null) {
                        return errorResultStage(ResultCodes.OTHER_DNS_ERROR, result.exception.getMessage());
                    } else if (result == null || result.soa == null) {
                        return errorResultStage(ResultCodes.NOT_FOUND, "Zone/SOA not found");
                    }

                    // Extract the primary NS domain name
                    Name primaryNsName = result.soa.getHost();

                    // Find all nameserver names
                    final var nameserversStage = findNameserversAsync(result.name);

                    return nameserversStage.thenComposeAsync(nameservers -> {
                        // Init the "Resolve IPs action" for the primary NS
                        final CompletionStage<Set<String>> primaryNsIpsStage = resolveIpsAsync(primaryNsName)
                                .thenApply(primaryNsIp -> primaryNsIp.stream()
                                        .map(InetAddress::getHostAddress).collect(Collectors.toSet()));

                        if (nameservers == null || nameservers.isEmpty()) {
                            // Nameservers are not available, return only the primary NS and its IPs
                            return primaryNsIpsStage
                                    .thenApply(primaryNsIpSet ->
                                            successResult(new ZoneInfo(result.name.toString(true),
                                                    new DNSData.SOARecord(result.soa),
                                                    publicSuffix,
                                                    registrySuffix,
                                                    primaryNsIpSet,
                                                    null,
                                                    null)));
                        }

                        // Filter out the primary NS to separate it from the secondaries
                        nameservers.remove(primaryNsName);
                        final var secondaryNsNameStrings = nameservers.stream()
                                .map(nsName -> nsName.toString(true)).collect(Collectors.toSet());


                        // Init the "Resolve IPs action" for each secondary NS
                        final Stream<CompletableFuture<Set<InetAddress>>> secondaryNsIpsFutures = nameservers.stream()
                                .map(this::resolveIpsAsync)
                                .map(CompletionStage::toCompletableFuture);

                        final var secondaryNsIpsFuturesArray = secondaryNsIpsFutures.toArray(CompletableFuture[]::new);

                        final var allSecondaryNsIpsStage =
                                CompletableFuture.allOf(secondaryNsIpsFuturesArray)
                                        .thenApply(unused -> {
                                            // When all secondary NS IPs are resolved, combine them into a single set
                                            final Set<String> allIps = new HashSet<>();
                                            for (var stage : secondaryNsIpsFuturesArray) {
                                                try {
                                                    @SuppressWarnings("unchecked")
                                                    var ips = (Set<InetAddress>) stage.get();
                                                    for (var ip : ips) {
                                                        allIps.add(ip.getHostAddress());
                                                    }
                                                } catch (Exception e) {
                                                    Logger.debug("Failed to resolve IPs for a secondary NS", e);
                                                    // Ignore the exception
                                                }
                                            }
                                            return allIps;
                                        });

                        // Combine all information into the ZoneInfo object
                        return primaryNsIpsStage.thenCombineAsync(allSecondaryNsIpsStage, (primaryIps, secondaryIps) ->
                                successResult(new ZoneInfo(result.name.toString(true),
                                        new DNSData.SOARecord(result.soa),
                                        publicSuffix,
                                        registrySuffix,
                                        primaryIps,
                                        secondaryNsNameStrings,
                                        secondaryIps)), _executor);
                    }, _executor);
                });
    }

    private LookupSession getLookupSession() {
        return LookupSession.builder()
                .resolver(_mainResolver)
                .executor(_executor)
                .build();
    }

    private static CompletionStage<ZoneResult> errorResultStage(int code, String message) {
        return CompletableFuture.completedFuture(new ZoneResult(code, message, Instant.now(), null));
    }

    private static ZoneResult successResult(ZoneInfo zoneInfo) {
        return new ZoneResult(ResultCodes.OK, null, Instant.now(), zoneInfo);
    }

    public CompletionStage<Set<Name>> findNameserversAsync(String domainName) {
        try {
            return findNameserversAsync(Name.fromString(domainName));
        } catch (TextParseException e) {
            Logger.warn("Invalid domain name {}", domainName, e);
            return CompletableFuture.completedFuture(Collections.emptySet());
        }
    }

    public CompletionStage<Set<Name>> findNameserversAsync(Name domainName) {
        var lookupSession = this.getLookupSession();
        return lookupSession.lookupAsync(domainName, Type.NS)
                .thenApply(ans -> ans.getRecords()
                        .stream().map(record -> ((NSRecord) record).getTarget())
                        .collect(Collectors.toSet()))
                .exceptionally(e -> {
                    Logger.warn("Failed to resolve A records for {}", domainName, e);
                    return Collections.emptySet();
                });
    }

    public CompletionStage<Set<InetAddress>> resolveIpsAsync(String domainName) {
        try {
            return resolveIpsAsync(Name.fromString(domainName));
        } catch (TextParseException e) {
            Logger.warn("Invalid domain name {}", domainName, e);
            return CompletableFuture.completedFuture(Collections.emptySet());
        }
    }

    public CompletionStage<Set<InetAddress>> resolveIpsAsync(Name domainName) {
        var lookupSession = this.getLookupSession();

        var aSession = lookupSession.lookupAsync(domainName, Type.A)
                .thenApply(ans -> ans.getRecords()
                        .stream().map(record -> ((ARecord) record).getAddress()))
                .exceptionally(e -> {
                    Logger.warn("Failed to resolve A records for {}", domainName, e);
                    return Stream.empty();
                });

        var aaaaSession = lookupSession.lookupAsync(domainName, Type.AAAA)
                .thenApply(ans -> ans.getRecords()
                        .stream().map(record -> ((AAAARecord) record).getAddress()))
                .exceptionally(e -> {
                    Logger.warn("Failed to resolve AAAA records for {}", domainName, e);
                    return Stream.empty();
                });

        return aSession.thenCombine(aaaaSession, Stream::concat)
                .thenApply(x -> x.collect(Collectors.toSet()));
    }
}
