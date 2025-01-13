package cz.vut.fit.domainradar;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;


public class CommonTest {

    @ParameterizedTest(name = "{index} => input={0}, expected={1}")
    @MethodSource("uniqueSortedLongArrayTestCases")
    void testUniqueSortedLongArray(long[] input, long[] expected, String description) {
        long[] actual = Common.uniqueSortedLongArray(input);
        assertArrayEquals(expected, actual, description);
    }

    private static Stream<Arguments> uniqueSortedLongArrayTestCases() {
        return Stream.of(
                Arguments.of(
                        new long[]{3, 4, 3, 5, 1, 4},
                        new long[]{1, 3, 4, 5},
                        "Should sort and de-duplicate a standard unsorted array with duplicates."
                ),
                Arguments.of(
                        new long[]{1, 2, 3},
                        new long[]{1, 2, 3},
                        "Should return the same array when input is already sorted and unique."
                ),
                Arguments.of(
                        new long[]{2, 2, 2, 2, 2},
                        new long[]{2},
                        "Should return a single element array when all values are identical."
                ),
                Arguments.of(
                        new long[]{},
                        new long[]{},
                        "Should return an empty array for an empty input."
                ),
                Arguments.of(
                        new long[]{-1, -3, -2, -1, 0},
                        new long[]{-3, -2, -1, 0},
                        "Should correctly sort negative numbers and remove duplicates."
                ),
                Arguments.of(
                        new long[]{5, -1, 3, -1, 5, 2, 2},
                        new long[]{-1, 2, 3, 5},
                        "Should sort a mix of positive and negative numbers and remove duplicates."
                )
        );
    }
}