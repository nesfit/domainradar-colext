package cz.vut.fit.domainradar.serialization;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.avro.AvroSchema;
import com.fasterxml.jackson.dataformat.avro.schema.AvroSchemaGenerator;
import cz.vut.fit.domainradar.Common;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.ConcurrentHashMap;

public class AvroSchemaContainer {
    private static final ConcurrentHashMap<String, AvroSchema> _schemas = new ConcurrentHashMap<>();
    private static final HashMap<String, AvroSchema> _preloadedSchemas = new HashMap<>();
    private static final Logger _logger = LoggerFactory.getLogger(AvroSchemaContainer.class);

    public static void loadSchemas(File directory) {
        var files = directory.listFiles();
        if (files == null)
            return;

        var parser = new Schema.Parser().setValidate(true);

        for (var file : files) {
            if (!file.isFile())
                continue;

            try {
                Schema raw = parser.parse(file);
                AvroSchema schema = new AvroSchema(raw);
                _preloadedSchemas.put(file.getName().replace(".avsc", ""), schema);
            } catch (IOException e) {
                _logger.warn("Error parsing schema file {}", file.getName());
            }
        }
    }

    public static AvroSchema get(final ObjectMapper mapper, final JavaType type) {
        var name = type.toString();
        return _schemas.computeIfAbsent(name, k -> {
            var label = Common.getTypeLabel(type);
            var schema = _preloadedSchemas.get(label);
            if (schema != null)
                return schema;

            _logger.warn("Generating schema for {}", label);
            AvroSchemaGenerator gen = new AvroSchemaGenerator();
            gen.enableLogicalTypes();

            try {
                mapper.acceptJsonFormatVisitor(type, gen);
            } catch (JsonMappingException e) {
                throw new RuntimeException(e);
            }

            return gen.getGeneratedSchema();
        });
    }

}
