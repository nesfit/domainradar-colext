package cz.vut.fit.domainradar.models;

public record StringPair(String first, String second) {
    public String domainName() {
        return first;
    }

    public String ip() {
        return second;
    }
}
