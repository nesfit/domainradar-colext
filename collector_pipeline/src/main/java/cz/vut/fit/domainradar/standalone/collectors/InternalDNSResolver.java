package cz.vut.fit.domainradar.standalone.collectors;

import com.google.common.net.InternetDomainName;
import cz.vut.fit.domainradar.models.dns.DNSData;
import cz.vut.fit.domainradar.models.dns.ZoneInfo;
import org.slf4j.LoggerFactory;
import org.xbill.DNS.*;
import org.xbill.DNS.lookup.LookupSession;

import java.net.InetAddress;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class InternalDNSResolver {
    private static final org.slf4j.Logger Logger = LoggerFactory.getLogger(InternalDNSResolver.class);

    private record ZoneLookupResult(Name name, SOARecord soa) {
    }

    private final Resolver _mainResolver;
    private final ExecutorService _executor;

    public InternalDNSResolver(Resolver resolver, ExecutorService executor) {
        _mainResolver = resolver;
        _executor = executor;
    }

    private LookupSession getLookupSession() {
        return LookupSession.builder()
                .resolver(_mainResolver)
                .executor(_executor)
                .build();
    }

    private static <T> CompletionStage<T> nullResult() {
        return CompletableFuture.completedFuture(null);
    }

    private static CompletionStage<ZoneInfo> makeResult(CompletionStage<ZoneLookupResult> lookupResult) {
        return lookupResult.thenApply(result -> {
            if (result == null) {
                return null;
            } else {
                return new ZoneInfo(new DNSData.SOARecord(result.soa.getHost().toString(true),
                        result.soa.getAdmin().toString(true),
                        Long.toString(result.soa.getSerial()),
                        result.soa.getRefresh(),
                        result.soa.getRetry(),
                        result.soa.getExpire(),
                        result.soa.getMinimum()), result.name.toString(true),
                        null, null, null);
            }
        });
    }

    public CompletionStage<ZoneInfo> getZoneInfo(String domainName) {
        final var lookupSession = getLookupSession();

        InternetDomainName guavaName;
        try {
            guavaName = InternetDomainName.from(domainName);
        } catch (IllegalArgumentException e) {
            Logger.info("Invalid domain name: {}", domainName, e);
            return nullResult();
        }

        // Check if the domain name has a public suffix (eTLD)
        if (!guavaName.hasPublicSuffix()) {
            Logger.debug("Domain name does not have a valid public suffix: {}", domainName);
            return nullResult();
        }

        Name baseName = Name.root;
        var components = guavaName.parts();
        if (guavaName.isPublicSuffix()) {
            // Resolve SOA for the suffix (start at the root)
            return zoneLookupStep(baseName, components, components.size() - 1,
                    lookupSession, null);
        } else {

        }


        return nullResult();
    }

    private CompletionStage<ZoneLookupResult> zoneLookupStep(Name baseName, List<String> componentsToTry,
                                                             int componentIndex, LookupSession session,
                                                             ZoneLookupResult previousResult) {
        if (componentIndex >= componentsToTry.length) {
            return CompletableFuture.completedFuture(null);
        }

        var name = Name.concatenate(baseName, componentsToTry[componentIndex]);

        return session.lookupAsync(name, Type.SOA)
                .thenComposeAsync(result -> {
                    var records = result.getRecords();
                    if (records.isEmpty()) {
                        // No SOA found, return the previous good result
                        return CompletableFuture.completedFuture(previousResult);
                    } else {
                        var lookupResult = new ZoneLookupResult(name, (SOARecord) records.getFirst());
                        return zoneLookupStep(baseName, componentsToTry, componentIndex + 1, session, lookupResult);
                    }
                });
    }


    public CompletionStage<Set<Name>> findNameservers(String domainName) {
        Name name;
        try {
            name = Name.fromString(domainName);
        } catch (TextParseException e) {
            Logger.warn("Invalid domain name {}", domainName, e);
            return CompletableFuture.completedFuture(Collections.emptySet());
        }

        var lookupSession = this.getLookupSession();
        return lookupSession.lookupAsync(name, Type.NS)
                .thenApply(ans -> ans.getRecords()
                        .stream().map(record -> ((NSRecord) record).getTarget())
                        .collect(Collectors.toSet()))
                .exceptionally(e -> {
                    Logger.warn("Failed to resolve A records for {}", domainName, e);
                    return Collections.emptySet();
                });
    }

    public CompletionStage<Set<InetAddress>> resolveIps(String domainName) {
        Name name;
        try {
            name = Name.fromString(domainName);
        } catch (TextParseException e) {
            Logger.warn("Invalid domain name {}", domainName, e);
            return CompletableFuture.completedFuture(Collections.emptySet());
        }

        var lookupSession = this.getLookupSession();

        var aSession = lookupSession.lookupAsync(name, Type.A)
                .thenApply(ans -> ans.getRecords()
                        .stream().map(record -> ((ARecord) record).getAddress()))
                .exceptionally(e -> {
                    Logger.warn("Failed to resolve A records for {}", domainName, e);
                    return Stream.empty();
                });

        var aaaaSession = lookupSession.lookupAsync(name, Type.AAAA)
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
