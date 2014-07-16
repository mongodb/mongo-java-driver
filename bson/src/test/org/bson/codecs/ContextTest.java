package org.bson.codecs;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ContextTest {

    @Test
    public void testDecoderContext() {
        DecoderContext dctx = DecoderContext.builder().addParameter("test", "value").build();
        assertEquals(dctx.getParameter("test"), "value");
    }

    @Test
    public void testEncoderContextCustomProperty() {
        EncoderContext ectx = EncoderContext.builder().addParameter("test", "value").build();
        assertEquals(ectx.getParameter("test"), "value");
    }
}
