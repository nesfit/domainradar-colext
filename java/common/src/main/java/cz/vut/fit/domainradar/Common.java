package cz.vut.fit.domainradar;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.cfg.MapperBuilder;
import com.fasterxml.jackson.databind.json.JsonMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;

import java.util.Arrays;

/**
 * Common utility functions and constants.
 *
 * @author Ondřej Ondryáš
 */
public final class Common {
    /**
     * Creates a new Jackson JSON {@link ObjectMapper} with the following settings:
     * <ul>
     *     <li>Include the JavaTimeModule to support Java 8 date/time datatypes.</li>
     *     <li>Include source locations in exceptions.</li>
     *     <li>Read/write date timestamps as milliseconds.</li>
     *     <li>Do not fail on unknown properties.</li>
     * </ul>
     *
     * @return a new {@link ObjectMapper} instance
     */
    public static MapperBuilder<? extends ObjectMapper, ?> makeMapper() {
        return JsonMapper.builder()
                .addModule(new JavaTimeModule())
                .configure(JsonParser.Feature.INCLUDE_SOURCE_IN_LOCATION, true)
                .configure(SerializationFeature.WRITE_DATE_TIMESTAMPS_AS_NANOSECONDS, false)
                .configure(DeserializationFeature.READ_DATE_TIMESTAMPS_AS_NANOSECONDS, false)
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    /**
     * Creates a logger for a specific pipeline component. The logger name will be created by concatenating
     * the class name, a dot, and the value of the static field {@code COMPONENT_NAME} in the class.
     *
     * @param clazz the class to get the logger for
     * @return a SLF4J {@link Logger} instance
     */
    public static Logger getComponentLogger(Class<?> clazz) {
        try {
            final String componentName = clazz.getField("COMPONENT_NAME")
                    .get(null).toString();
            return org.slf4j.LoggerFactory.getLogger(clazz.getName() + "." + componentName);
        } catch (IllegalAccessException | NoSuchFieldException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Returns a sorted array containing the unique elements of the specified input array.
     * <p>
     * <b>Note:</b> The original input array is modified (sorted) in-place during the operation.
     *
     * @param input a non-null array of {@code long} values; may contain duplicates and unsorted elements.
     * @return a new array containing the unique elements from the input, sorted in ascending order.
     * @throws NullPointerException if the provided input array is {@code null}.
     */
    public static long[] uniqueSortedLongArray(long @NotNull [] input) {
        Arrays.sort(input);

        // Deduplicate
        int uniqueCount = 0;
        for (int i = 0; i < input.length; i++) {
            if (i == 0 || input[i] != input[i - 1]) {
                input[uniqueCount++] = input[i];
            }
        }

        // Return array with unique values only
        return Arrays.copyOf(input, uniqueCount);
    }
}
