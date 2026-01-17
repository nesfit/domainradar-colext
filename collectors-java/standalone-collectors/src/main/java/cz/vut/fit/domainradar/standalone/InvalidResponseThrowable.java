package cz.vut.fit.domainradar.standalone;

public class InvalidResponseThrowable extends Throwable {
    public InvalidResponseThrowable(String message) {
        super(message, null, true, false);
    }
}
