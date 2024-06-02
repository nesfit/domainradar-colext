package cz.vut.fit.domainradar;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.cfg.MapperBuilder;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.dataformat.avro.AvroMapper;
import com.fasterxml.jackson.dataformat.avro.jsr310.AvroJavaTimeModule;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

public final class Common {
    public static MapperBuilder<? extends ObjectMapper, ?> makeMapper() {
        return AvroMapper.builder()
                .addModule(new AvroJavaTimeModule())
                .configure(JsonParser.Feature.INCLUDE_SOURCE_IN_LOCATION, true)
                .configure(SerializationFeature.WRITE_DATE_TIMESTAMPS_AS_NANOSECONDS, false)
                .configure(DeserializationFeature.READ_DATE_TIMESTAMPS_AS_NANOSECONDS, false)
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        /*
        return JsonMapper.builder()
                .addModule(new JavaTimeModule())
                .configure(JsonParser.Feature.INCLUDE_SOURCE_IN_LOCATION, true)
                .configure(SerializationFeature.WRITE_DATE_TIMESTAMPS_AS_NANOSECONDS, false)
                .configure(DeserializationFeature.READ_DATE_TIMESTAMPS_AS_NANOSECONDS, false)
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
         */
    }

    public static String getTypeLabel(JavaType type) {
        return type.toString()
                .replace("[simple type, class ", "")
                .replace("]", "")
                .replace('<', '%')
                .replace('>', '%');
    }
}
