package cz.vut.fit.domainradar.standalone;

public class InvalidResponseCodeThrowable extends Throwable {
    public InvalidResponseCodeThrowable(int code) {
        super("HTTP " + code, null, false, false);
    }
}
