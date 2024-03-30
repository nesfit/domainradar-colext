package cz.vut.fit.domainradar.models;

public record IPToProcess(String domainName, String ip) implements StringPair {

    @Override
    public String first() {
        return domainName;
    }

    @Override
    public String second() {
        return ip;
    }
}
