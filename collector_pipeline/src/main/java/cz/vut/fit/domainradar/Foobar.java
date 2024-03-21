package cz.vut.fit.domainradar;

import com.google.common.net.InternetDomainName;

public class Foobar {
    public static void main(String[] args) {
        var names = new InternetDomainName[]{
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
                InternetDomainName.from("asd-ewqe.dev.adobeaemcloud.com")
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
    }
}
