package cz.vut.fit.domainradar;

import com.fasterxml.jackson.databind.json.JsonMapper;
import com.google.common.net.InternetDomainName;
import cz.vut.fit.domainradar.standalone.collectors.DNSCollector;
import cz.vut.fit.domainradar.standalone.collectors.InternalDNSResolver;
import org.xbill.DNS.SimpleResolver;
import org.xbill.DNS.TextParseException;

import java.net.UnknownHostException;
import java.util.Properties;
import java.util.concurrent.Executors;

public class Foobar {
    public static void main(String[] args) throws InterruptedException {
        var names = new InternetDomainName[]{
//                InternetDomainName.from("email.seznam.cz"),
//                InternetDomainName.from("www.merlin.fit.vutbr.cz"),
//                InternetDomainName.from("merlin.fit.vutbr.cz"),
//                InternetDomainName.from("www.fit.vut.cz"),
//                InternetDomainName.from("fit.vut.cz"),
//                InternetDomainName.from("vut.cz"),
//                InternetDomainName.from("cz"),
//                InternetDomainName.from("ondryaso.eu"),
//                InternetDomainName.from("www.ondryaso.eu"),
//                InternetDomainName.from("jp"),
//                InternetDomainName.from("hokkaido.jp"),
//                InternetDomainName.from("hakodate.hokkaido.jp"),
//                InternetDomainName.from("co.uk"),
//                InternetDomainName.from("cz"),
//                InternetDomainName.from("cesnet.cz"),
//                InternetDomainName.from("xyz.co.uk"),
               InternetDomainName.from("www.blogspot.com"),
//                InternetDomainName.from("blogspot.com"),
//                InternetDomainName.from("adobeaemcloud.com"),
//                InternetDomainName.from("sadasd.asdasd.adobeaemcloud.com"),
//                InternetDomainName.from("asd-ewqe.dev.adobeaemcloud.com"),
//                InternetDomainName.from("invalid"),
//                InternetDomainName.from("something.invalid")
        };

        for (var name : names) {
            System.out.println(name);
            name.parts().forEach(x -> System.out.println("-> " + x));

            try {
                System.out.println("Public suffix: " + name.publicSuffix());
            } catch (IllegalStateException e) {
                System.out.println("No public suffix");
            }
            try {
                System.out.println("Top private domain: " + name.topPrivateDomain());
            } catch (IllegalStateException e) {
                System.out.println("No top private domain");
            }
            try {
                System.out.println("Registry suffix: " + name.registrySuffix());
            } catch (IllegalStateException e) {
                System.out.println("No registry suffix");
            }
            try {
                System.out.println("Top domain under registry suffix: " + name.topDomainUnderRegistrySuffix());
            } catch (IllegalStateException e) {
                System.out.println("No top domain under registry suffix");
            }

            System.out.println();
        }

        var ex = Executors.newVirtualThreadPerTaskExecutor();
        try {
            var dns = new InternalDNSResolver(ex, new Properties());
            var col = new DNSCollector(JsonMapper.builder().build(), "", new Properties());

            for (var name : names) {
                var zoneInfo = dns.getZoneInfo(name.toString())
                        .toCompletableFuture().join();

                System.out.println(name);
                System.out.println(zoneInfo);

                if (zoneInfo.zone() == null) {
                    System.out.println("No zone");
                    System.out.println();
                    continue;
                }
                var scanner = dns.makeScanner(name.toString(), zoneInfo.zone());
                var data = scanner.scan(null).toCompletableFuture().join();

                if (data != null) {
                    col.runTlsResolve(name.toString(), data, ex);
                }
            }
        } catch (UnknownHostException | TextParseException e) {
            throw new RuntimeException(e);
        }

        //ex.awaitTermination(20000, TimeUnit.MILLISECONDS);
    }
}
