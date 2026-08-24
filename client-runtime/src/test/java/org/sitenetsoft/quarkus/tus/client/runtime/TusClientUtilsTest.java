package org.sitenetsoft.quarkus.tus.client.runtime;

import org.junit.jupiter.api.Test;
import org.sitenetsoft.quarkus.tus.client.runtime.error.*;

import java.util.Map;
import java.util.TreeMap;

import static org.junit.jupiter.api.Assertions.*;

class TusClientUtilsTest {

    @Test
    void encodesMetadataAsBase64Pairs() {
        var encoded = TusClientUtils.encodeMetadata(new TreeMap<>(Map.of("filename", "cat.png", "flag", "")));
        assertEquals("filename Y2F0LnBuZw==,flag", encoded);
    }

    @Test
    void emptyMetadataEncodesToEmptyString() {
        assertEquals("", TusClientUtils.encodeMetadata(Map.of()));
    }

    @Test
    void mapsStatusesToTypedExceptions() {
        assertInstanceOf(TusOffsetMismatchException.class, TusClientUtils.fromStatus(409, false));
        assertInstanceOf(TusUploadNotFoundException.class, TusClientUtils.fromStatus(404, false));
        assertTrue(((TusUploadNotFoundException) TusClientUtils.fromStatus(410, true)).knownExpired());
        assertInstanceOf(TusVersionMismatchException.class, TusClientUtils.fromStatus(412, false));
        assertInstanceOf(TusPayloadTooLargeException.class, TusClientUtils.fromStatus(413, false));
        assertInstanceOf(TusChecksumMismatchException.class, TusClientUtils.fromStatus(460, false));
        assertEquals(503, ((TusServerErrorException) TusClientUtils.fromStatus(503, false)).status());
        assertInstanceOf(TusProtocolException.class, TusClientUtils.fromStatus(302, false));
    }

    @Test
    void rejectsMetadataKeyWithSpace() {
        var ex = assertThrows(IllegalArgumentException.class,
            () -> TusClientUtils.encodeMetadata(Map.of("file name", "test.txt")));
        assertTrue(ex.getMessage().contains("file name"));
    }

    @Test
    void rejectsMetadataKeyWithComma() {
        var ex = assertThrows(IllegalArgumentException.class,
            () -> TusClientUtils.encodeMetadata(Map.of("a,b", "value")));
        assertTrue(ex.getMessage().contains("a,b"));
    }

    @Test
    void rejectsMetadataKeyWithNewline() {
        var ex = assertThrows(IllegalArgumentException.class,
            () -> TusClientUtils.encodeMetadata(Map.of("key\nvalue", "test")));
        assertTrue(ex.getMessage().contains("key\nvalue"));
    }

    @Test
    void rejectsNullMetadataKey() {
        var map = new java.util.HashMap<String, String>();
        map.put(null, "value");
        assertThrows(IllegalArgumentException.class,
            () -> TusClientUtils.encodeMetadata(map));
    }

    @Test
    void rejectsEmptyMetadataKey() {
        var ex = assertThrows(IllegalArgumentException.class,
            () -> TusClientUtils.encodeMetadata(Map.of("", "value")));
        assertTrue(ex.getMessage().contains("empty"));
    }
}
