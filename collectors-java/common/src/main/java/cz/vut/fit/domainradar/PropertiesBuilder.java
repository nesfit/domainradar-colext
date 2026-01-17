package cz.vut.fit.domainradar;

import java.util.Properties;

public final class PropertiesBuilder {
    private final Properties _properties;

    public PropertiesBuilder() {
        _properties = new Properties();
    }

    public PropertiesBuilder add(String key, String value) {
        _properties.put(key, value);
        return this;
    }

    public Properties get() {
        return _properties;
    }
}