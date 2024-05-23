package cz.vut.fit.domainradar.standalone.collectors.dns;

public final class RecordTypeFlags {
    public static final int MASK = 0b111111;
    public static final int A = 0b100000;
    public static final int A_CLEAR = ~A & MASK;
    public static final int AAAA = 0b010000;
    public static final int AAAA_CLEAR = ~AAAA & MASK;
    public static final int CNAME = 0b001000;
    public static final int CNAME_CLEAR = ~CNAME & MASK;
    public static final int MX = 0b000100;
    public static final int MX_CLEAR = ~MX & MASK;
    public static final int NS = 0b000010;
    public static final int NS_CLEAR = ~NS & MASK;
    public static final int TXT = 0b000001;
    public static final int TXT_CLEAR = ~TXT & MASK;
}
