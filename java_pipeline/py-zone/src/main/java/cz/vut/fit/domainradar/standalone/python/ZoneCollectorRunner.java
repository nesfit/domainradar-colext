package cz.vut.fit.domainradar.standalone.python;

import org.graalvm.polyglot.Context;

public class ZoneCollectorRunner {
    public static void main(String[] args) {
        try (Context context = Context.create()) {
            context.eval("python", "print('Hello from GraalPy!')");
        }
    }
}
