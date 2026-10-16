package org.data_platform;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class MeasurementJobTest {
    @Test void acceptsNumericMeasurements() {
        assertEquals("sensor_1", MeasurementJob.key("{\"event_id\":\"one\",\"sensor\":\"sensor_1\",\"value\":1.5}"));
    }
    @Test void rejectsMissingFieldsStringsAndOutOfRangeNumbers() {
        for (String raw : new String[]{"null", "[]", "broken", "{}",
                "{\"event_id\":\"one\",\"sensor\":\"s\",\"value\":\"1\"}",
                "{\"event_id\":\"one\",\"sensor\":\"s\",\"value\":1e50}"})
            assertThrows(IllegalArgumentException.class, () -> MeasurementJob.parse(raw));
    }
    @Test void rejectsPathLikeIdentifiers() {
        assertThrows(IllegalArgumentException.class, () -> MeasurementJob.parse("{\"event_id\":\"../one\",\"sensor\":\"s\",\"value\":1}"));
    }
    @Test void rejectionDoesNotExposeInputAndHasStableIdentity() {
        String raw = "private arbitrary payload";
        assertFalse(MeasurementJob.rejected(raw,"invalid_measurement").contains(raw));
        assertEquals(MeasurementJob.rejected(raw,"invalid_measurement"), MeasurementJob.rejected(raw,"invalid_measurement"));
        assertNotEquals(MeasurementJob.digest(raw), MeasurementJob.digest(raw+"2"));
    }
}
