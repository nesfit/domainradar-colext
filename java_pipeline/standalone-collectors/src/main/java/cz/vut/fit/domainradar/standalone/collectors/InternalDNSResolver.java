package cz.vut.fit.domainradar.standalone.collectors;

import com.google.common.net.InetAddresses;
import com.google.common.net.InternetDomainName;
import cz.vut.fit.domainradar.CollectorConfig;
import cz.vut.fit.domainradar.models.ResultCodes;
import cz.vut.fit.domainradar.models.dns.ZoneInfo;
import cz.vut.fit.domainradar.models.results.ZoneResult;
import org.jetbrains.annotations.NotNull;
import org.slf4j.LoggerFactory;
import org.xbill.DNS.*;
import org.xbill.DNS.lookup.LookupResult;
import org.xbill.DNS.lookup.LookupSession;
import org.xbill.DNS.lookup.NoSuchDomainException;

import java.io.IOException;
import java.net.*;
import java.nio.channels.UnsupportedAddressTypeException;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
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

    public record TTLTuple<T>(long ttl, T value) {
        public static <T> TTLTuple<T> of(LookupResult result, T value) {
            return new TTLTuple<>(result.getRecords().getFirst().getTTL(), value);
        }

        public static <T extends Collection<?>> TTLTuple<T> of(LookupResult result, T value) {
            if (value.isEmpty())
                return TTLTuple.ofNull();

            return new TTLTuple<>(result.getRecords().getFirst().getTTL(), value);
        }

        public static <T> TTLTuple<T> ofNull() {
            return new TTLTuple<>(-1, null);
        }
    }

    public class DNSScanner {
        private final Name _name;
        private final ZoneInfo _zoneInfo;
        private LookupSession _primaryLookupSession, _fallbackLookupSession;
        private boolean _ipv6EnabledLocally = true;

        private DNSScanner(Name name, ZoneInfo zoneInfo) {
            var nameNotNull = Objects.requireNonNull(name);
            _zoneInfo = Objects.requireNonNull(zoneInfo);

            try {
                _name = Name.concatenate(nameNotNull, Name.root);
            } catch (NameTooLongException e) {
                // Shouldn't happen
                throw new RuntimeException(e);
            }
        }

        public CompletionStage<TTLTuple<Set<String>>> resolveA() {
            return this.resolve(Type.A)
                    .thenApply(result -> {
                        if (result == null) {
                            return TTLTuple.ofNull();
                        }

                        return TTLTuple.of(result,
                                result.getRecords().stream()
                                        // Sanity check: you never know what the DNS returns
                                        .filter(record -> record.getType() == Type.A)
                                        .map(record -> ((ARecord) record))
                                        .filter(record -> record.getName().equals(_name))
                                        .map(record -> record.getAddress().getHostAddress())
                                        .collect(Collectors.toSet()));
                    });

        }

        public CompletionStage<TTLTuple<Set<String>>> resolveAAAA() {
            return this.resolve(Type.AAAA)
                    .thenApply(result -> {
                        if (result == null) {
                            return TTLTuple.ofNull();
                        }

                        return TTLTuple.of(result, result.getRecords().stream()
                                // Sanity check: you never know what the DNS returns
                                .filter(record -> record.getType() == Type.AAAA)
                                .map(record -> (AAAARecord) record)
                                .filter(record -> record.getName().equals(_name))
                                .map(record -> record.getAddress().getHostAddress())
                                .collect(Collectors.toSet()));
                    });
        }

        public CompletionStage<TTLTuple<cz.vut.fit.domainradar.models.dns.CNAMERecord>> resolveCNAME() {
            return this.resolve(Type.CNAME)
                    .thenCompose(result -> {
                        if (result == null) {
                            return CompletableFuture.completedFuture(TTLTuple.ofNull());
                        }

                        // A CNAME response may also contain the A records for the target
                        final var record = (CNAMERecord) result.getRecords().stream()
                                .filter(resultRecord -> resultRecord.getType() == Type.CNAME)
                                .findFirst()
                                .orElse(null);

                        if (record == null) {
                            return CompletableFuture.completedFuture(TTLTuple.ofNull());
                        }

                        var resolveIpsStage = InternalDNSResolver.this.resolveIpsAsync(record.getTarget());

                        return resolveIpsStage.thenApply(ips -> TTLTuple.of(result, new cz.vut.fit.domainradar.models.dns.CNAMERecord(
                                record.getTarget().toString(true),
                                ips.stream().map(InetAddress::getHostAddress).collect(Collectors.toList())
                        )));
                    });
        }

        public CompletionStage<TTLTuple<List<cz.vut.fit.domainradar.models.dns.MXRecord>>> resolveMX() {
            return this.resolve(Type.MX)
                    .thenCompose(result -> {
                        if (result == null) {
                            return CompletableFuture.completedFuture(TTLTuple.ofNull());
                        }

                        var records = result.getRecords().stream()
                                // Sanity check: you never know what the DNS returns
                                .filter(record -> record.getType() == Type.MX)
                                .map(record -> (MXRecord) record)
                                .toList();

                        var stages = records.stream()
                                .map(MXRecord::getTarget)
                                .map(InternalDNSResolver.this::resolveIpsAsync)
                                .map(CompletionStage::toCompletableFuture)
                                .toArray(CompletableFuture[]::new);

                        return CompletableFuture.allOf(stages)
                                .thenApply(unused -> {
                                    var returnRecords = new ArrayList<cz.vut.fit.domainradar.models.dns.MXRecord>();
                                    for (var i = 0; i < records.size(); i++) {
                                        final var inRecord = records.get(i);
                                        //noinspection unchecked
                                        returnRecords.add(new cz.vut.fit.domainradar.models.dns.MXRecord(
                                                inRecord.getTarget().toString(true),
                                                inRecord.getPriority(),
                                                ((CompletableFuture<Set<InetAddress>>) stages[i]).join().stream()
                                                        .map(InetAddress::getHostAddress)
                                                        .collect(Collectors.toList())));
                                    }
                                    return TTLTuple.of(result, returnRecords);
                                });
                    });
        }

        public CompletionStage<TTLTuple<List<cz.vut.fit.domainradar.models.dns.NSRecord>>> resolveNS() {
            return this.resolve(Type.NS)
                    .thenCompose(result -> {
                        if (result == null) {
                            return CompletableFuture.completedFuture(TTLTuple.ofNull());
                        }

                        var records = result.getRecords().stream()
                                // Sanity check: you never know what the DNS returns
                                .filter(record -> record.getType() == Type.NS)
                                .map(record -> (NSRecord) record)
                                .toList();

                        var stages = records.stream()
                                .map(NSRecord::getTarget)
                                .map(InternalDNSResolver.this::resolveIpsAsync)
                                .map(CompletionStage::toCompletableFuture)
                                .toArray(CompletableFuture[]::new);

                        return CompletableFuture.allOf(stages)
                                .thenApply(unused -> {
                                    var returnRecords = new ArrayList<cz.vut.fit.domainradar.models.dns.NSRecord>();
                                    for (var i = 0; i < records.size(); i++) {
                                        //noinspection unchecked
                                        returnRecords.add(new cz.vut.fit.domainradar.models.dns.NSRecord(
                                                records.get(i).getTarget().toString(true),
                                                ((CompletableFuture<Set<InetAddress>>) stages[i]).join().stream()
                                                        .map(InetAddress::getHostAddress)
                                                        .collect(Collectors.toList())));
                                    }
                                    return TTLTuple.of(result, returnRecords);
                                });
                    });
        }

        public CompletionStage<TTLTuple<List<String>>> resolveTXT() {
            return this.resolve(Type.TXT)
                    .thenApply(result -> {
                        if (result == null) {
                            return TTLTuple.ofNull();
                        }

                        return TTLTuple.of(result, result.getRecords().stream()
                                .filter(record -> record.getType() == Type.TXT)
                                .flatMap(record -> ((TXTRecord) record).getStrings().stream())
                                .collect(Collectors.toList()));
                    });
        }

        private CompletionStage<LookupResult> resolve(int type) {
            // A wrapper for resolveUnsafe that handles exceptions thrown outside the
            // CompletionStage chain
            try {
                return resolveUnsafe(type);
            } catch (UnsupportedAddressTypeException unused) {
                if (_ipv6EnabledLocally) {
                    _ipv6EnabledLocally = false;
                    return resolve(type);
                } else {
                    return CompletableFuture.completedFuture(null);
                }
            } catch (Exception e) {
                Logger.error("Top-level exception when resolving record type {} for {}", type, _name, e);
                return CompletableFuture.completedFuture(null);
            }
        }

        private CompletionStage<LookupResult> resolveUnsafe(int type) {
            // TODO: log stuff
            var lookupSession = getPrimaryLookupSession();

            // If we couldn't create a session from the primary NSs, use the main resolver
            // In this case, we won't bother trying with the secondary NSs
            final var isMain = lookupSession == null;
            if (isMain) {
                lookupSession = getLookupSession();
            }

            // Do a lookup with the primary or main resolver
            return lookupSession.lookupAsync(_name, type)
                    .exceptionallyCompose(e -> {
                        // Extract the actual exception
                        if (e instanceof CompletionException) {
                            e = e.getCause();
                        }

                        // IOExceptions are thrown when the query fails for a network reason
                        // In this case, we want to try another of the domain's NSs
                        // but only if we didn't use the main resolver
                        if (e instanceof IOException) {
                            if (isMain) {
                                return CompletableFuture.completedFuture(null);
                            }

                            var secondaryLookupSession = getSecondaryLookupSession();
                            if (secondaryLookupSession == null) {
                                return CompletableFuture.completedFuture(null);
                            }

                            return getSecondaryLookupSession().lookupAsync(_name, type)
                                    .exceptionallyCompose(e2 -> CompletableFuture.completedFuture(null));
                        } else {
                            return CompletableFuture.completedFuture(null);
                        }
                    })
                    .thenApply(result -> {
                        // If we got no records in the response, return null (as expected by the lookup methods)
                        if (result == null || result.getRecords().isEmpty()) {
                            return null;
                        } else {
                            return result;
                        }
                    });
        }


        private LookupSession getPrimaryLookupSession() {
            if (_primaryLookupSession != null) {
                return _primaryLookupSession;
            }

            return _primaryLookupSession = getLookupSessionForNameservers(_zoneInfo.primaryNameserverIps());
        }

        private LookupSession getSecondaryLookupSession() {
            if (_fallbackLookupSession != null) {
                return _fallbackLookupSession;
            }

            return _fallbackLookupSession = getLookupSessionForNameservers(_zoneInfo.secondaryNameserverIps());
        }

        private LookupSession getLookupSessionForNameservers(Set<String> nameservers) {
            if (nameservers == null || nameservers.isEmpty()) {
                return null;
            }

            try {
                String[] nameserversToUse;
                if (_ipv6Enabled && _ipv6EnabledLocally) {
                    nameserversToUse = nameservers.toArray(String[]::new);
                } else {
                    nameserversToUse = nameservers.stream()
                            .filter(x -> InetAddresses.forString(x) instanceof Inet4Address)
                            .toArray(String[]::new);
                }

                var resolver = new ExtendedResolver(nameserversToUse);
                resolver.setRetries(_perDomainNSRetries);
                resolver.setTimeout(_perDomainNSTimeout.multipliedBy(
                        (long) _perDomainNSRetries * nameserversToUse.length));
                resolver.setLoadBalance(true);
                resolver.setTCP(false);

                for (var inResolver : resolver.getResolvers()) {
                    inResolver.setTimeout(_perDomainNSTimeout);
                }

                return LookupSession.builder()
                        .executor(_executor)
                        .resolver(resolver)
                        .build();
            } catch (UnknownHostException e) {
                return null;
            }
        }
    }

    public static ExtendedResolver makeMainResolver(Properties properties) throws UnknownHostException {
        var dnsServers = properties.getProperty(CollectorConfig.DNS_MAIN_RESOLVER_IPS_CONFIG,
                CollectorConfig.DNS_MAIN_RESOLVER_IPS_DEFAULT).split(",");
        var randomize = Boolean.parseBoolean(properties.getProperty(
                CollectorConfig.DNS_MAIN_RESOLVER_RANDOMIZE_CONFIG, CollectorConfig.DNS_MAIN_RESOLVER_RANDOMIZE_DEFAULT));
        var timeoutForEach = Duration.ofSeconds(Integer.parseInt(properties.getProperty(
                CollectorConfig.DNS_MAIN_RESOLVER_TIMEOUT_PER_NS_MS_CONFIG, CollectorConfig.DNS_MAIN_RESOLVER_TIMEOUT_PER_NS_MS_DEFAULT)));
        var retries = Integer.parseInt(properties.getProperty(
                CollectorConfig.DNS_MAIN_RESOLVER_RETRIES_CONFIG, CollectorConfig.DNS_MAIN_RESOLVER_RETRIES_DEFAULT));

        if (randomize) {
            Collections.shuffle(Arrays.asList(dnsServers));
        }

        var resolver = new ExtendedResolver(dnsServers);

        for (var inResolver : resolver.getResolvers()) {
            inResolver.setTimeout(timeoutForEach);
        }

        resolver.setRetries(retries);
        resolver.setTimeout(timeoutForEach.multipliedBy((long) dnsServers.length * retries));
        resolver.setLoadBalance(Boolean.parseBoolean(properties.getProperty(
                CollectorConfig.DNS_MAIN_RESOLVER_ROUND_ROBIN_CONFIG, CollectorConfig.DNS_MAIN_RESOLVER_ROUND_ROBIN_DEFAULT)));

        return resolver;
    }

    public static boolean isIPv6Available() throws SocketException {
        final var interfaces = NetworkInterface.getNetworkInterfaces();
        while (interfaces.hasMoreElements()) {
            for (InterfaceAddress interfaceAddress : interfaces.nextElement().getInterfaceAddresses()) {
                final InetAddress ip = interfaceAddress.getAddress();
                if (ip.isLoopbackAddress() || ip instanceof Inet4Address) {
                    continue;
                }
                return true;
            }
        }
        return false;
    }

    private final Resolver _mainResolver;
    private final ExecutorService _executor;
    private final boolean _ipv6Enabled;

    private final Duration _perDomainNSTimeout;
    private final int _perDomainNSRetries;

    public InternalDNSResolver(@NotNull ExecutorService executor,
                               @NotNull Properties properties) throws UnknownHostException {
        _mainResolver = makeMainResolver(properties);
        _executor = executor;

        _perDomainNSRetries = Integer.parseInt(properties.getProperty(
                CollectorConfig.DNS_RETRIES_PER_NS_CONFIG,
                CollectorConfig.DNS_RETRIES_PER_NS_DEFAULT));
        _perDomainNSTimeout = Duration.ofMillis(Integer.parseInt(properties.getProperty(
                CollectorConfig.DNS_TIMEOUT_PER_NS_MS_CONFIG,
                CollectorConfig.DNS_TIMEOUT_PER_NS_MS_DEFAULT)));

        boolean ipv6Enabled;
        try {
            ipv6Enabled = isIPv6Available();
        } catch (SocketException unused) {
            ipv6Enabled = false;
        }
        _ipv6Enabled = ipv6Enabled;
    }

    public DNSScanner makeScanner(String name, ZoneInfo zoneInfo) throws TextParseException {
        return new DNSScanner(Name.fromString(name), zoneInfo);
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
                        if (exc instanceof NoSuchDomainException) {
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

    private static cz.vut.fit.domainradar.models.dns.SOARecord makeSoa(org.xbill.DNS.SOARecord xbillDnsRecord) {
        return new cz.vut.fit.domainradar.models.dns.SOARecord(xbillDnsRecord.getHost().toString(true),
                xbillDnsRecord.getAdmin().toString(true),
                Long.toString(xbillDnsRecord.getSerial()),
                xbillDnsRecord.getRefresh(),
                xbillDnsRecord.getRetry(),
                xbillDnsRecord.getExpire(),
                xbillDnsRecord.getMinimum());
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
                                                    makeSoa(result.soa),
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
                                        makeSoa(result.soa),
                                        publicSuffix,
                                        registrySuffix,
                                        primaryIps,
                                        secondaryNsNameStrings,
                                        secondaryIps)), _executor);
                    }, _executor);
                });
    }

    public CompletionStage<Set<Name>> findNameserversAsync(Name domainName) {
        var lookupSession = this.getLookupSession();
        return lookupSession.lookupAsync(domainName, Type.NS)
                .thenApply(ans -> ans.getRecords()
                        .stream().map(record -> ((NSRecord) record).getTarget())
                        .collect(Collectors.toSet()))
                .exceptionally(e -> {
                    if (e.getCause() instanceof NoSuchDomainException) {
                        Logger.debug("No NS records found for {} (no such domain)", domainName);
                    } else {
                        Logger.warn("Failed to resolve NS records for {}", domainName, e);
                    }
                    return Collections.emptySet();
                });
    }

    public CompletionStage<Set<InetAddress>> resolveIpsAsync(Name domainName) {
        var lookupSession = this.getLookupSession();

        var aSession = lookupSession.lookupAsync(domainName, Type.A)
                .thenApply(ans -> ans.getRecords()
                        .stream().map(record -> ((ARecord) record).getAddress()))
                .exceptionally(e -> {
                    if (e.getCause() instanceof NoSuchDomainException) {
                        Logger.debug("No A records found for {} (no such domain)", domainName);
                    } else {
                        Logger.warn("Failed to resolve A records for {}", domainName, e);
                    }
                    return Stream.empty();
                });

        var aaaaSession = lookupSession.lookupAsync(domainName, Type.AAAA)
                .thenApply(ans -> ans.getRecords()
                        .stream().map(record -> ((AAAARecord) record).getAddress()))
                .exceptionally(e -> {
                    if (e.getCause() instanceof NoSuchDomainException) {
                        Logger.debug("No AAAA records found for {} (no such domain)", domainName);
                    } else {
                        Logger.warn("Failed to resolve AAAA records for {}", domainName, e);
                    }
                    return Stream.empty();
                });

        return aSession.thenCombine(aaaaSession, Stream::concat)
                .thenApply(x -> x.collect(Collectors.toSet()));
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
}
