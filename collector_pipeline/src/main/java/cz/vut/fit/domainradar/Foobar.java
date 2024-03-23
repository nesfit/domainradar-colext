package cz.vut.fit.domainradar;

import com.google.common.net.InternetDomainName;
import cz.vut.fit.domainradar.standalone.collectors.InternalDNSResolver;
import org.xbill.DNS.SimpleResolver;

import java.net.UnknownHostException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Foobar {
    public static void main(String[] args) throws InterruptedException {
        var names = new InternetDomainName[]{
                InternetDomainName.from("email.seznam.cz"),
                InternetDomainName.from("www.merlin.fit.vut.cz"),
                InternetDomainName.from("www.fit.vut.cz"),
                InternetDomainName.from("fit.vut.cz"),
                InternetDomainName.from("vut.cz"),
                InternetDomainName.from("cz"),
                /*
                InternetDomainName.from("jp"),
                InternetDomainName.from("hokkaido.jp"),
                InternetDomainName.from("hakodate.hokkaido.jp"),
                InternetDomainName.from("co.uk"),
                InternetDomainName.from("cz"),
                InternetDomainName.from("cesnet.cz"),
                InternetDomainName.from("fit.vut.cz"),
                InternetDomainName.from("xyz.co.uk"),
                InternetDomainName.from("www.blogspot.com"),
                InternetDomainName.from("blogspot.com"),
                InternetDomainName.from("adobeaemcloud.com"),
                InternetDomainName.from("sadasd.asdasd.adobeaemcloud.com"),
                InternetDomainName.from("asd-ewqe.dev.adobeaemcloud.com"),
                InternetDomainName.from("invalid"),
                InternetDomainName.from("something.invalid"),
                 */
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
            var resolver = new SimpleResolver("1.1.1.1");
            var dns = new InternalDNSResolver(resolver, ex);

            for (var name : names) {
                dns.getZoneInfo(name.toString())
                        .thenAcceptAsync(x -> {
                            synchronized (ex) {
                                System.out.print(name + ": ");
                                System.out.println(x);
                            }
                        }, ex).toCompletableFuture().join();

            }
        } catch (UnknownHostException e) {
            throw new RuntimeException(e);
        }

        //ex.awaitTermination(20000, TimeUnit.MILLISECONDS);
    }
}
