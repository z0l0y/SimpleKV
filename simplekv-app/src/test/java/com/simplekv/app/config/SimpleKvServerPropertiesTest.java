package com.simplekv.app.config;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SimpleKvServerPropertiesTest {

    @Test
    void shouldExposeDefaultsAndSetters() {
        SimpleKvServerProperties properties = new SimpleKvServerProperties();

        assertEquals("127.0.0.1", properties.getHost());
        assertEquals(7379, properties.getPort());
        assertEquals(128, properties.getBacklog());
        assertTrue(properties.isBannerEnabled());

        properties.setHost("0.0.0.0");
        properties.setPort(6380);
        properties.setBacklog(256);
        properties.setBannerEnabled(false);

        assertEquals("0.0.0.0", properties.getHost());
        assertEquals(6380, properties.getPort());
        assertEquals(256, properties.getBacklog());
        assertEquals(false, properties.isBannerEnabled());
    }
}
