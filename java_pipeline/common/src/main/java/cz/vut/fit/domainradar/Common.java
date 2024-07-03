package cz.vut.fit.domainradar;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.cfg.MapperBuilder;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.slf4j.Logger;

public final class Common {
    public static MapperBuilder<? extends ObjectMapper, ?> makeMapper() {
        return JsonMapper.builder()
                .addModule(new JavaTimeModule())
                .configure(JsonParser.Feature.INCLUDE_SOURCE_IN_LOCATION, true)
                .configure(SerializationFeature.WRITE_DATE_TIMESTAMPS_AS_NANOSECONDS, false)
                .configure(DeserializationFeature.READ_DATE_TIMESTAMPS_AS_NANOSECONDS, false)
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    public static Logger getComponentLogger(Class<?> clazz) {
        try {
            final String componentName = clazz.getField("COMPONENT_NAME")
                    .get(null).toString();
            return org.slf4j.LoggerFactory.getLogger(clazz.getName() + "." + componentName);
        } catch (IllegalAccessException | NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
    }
}
