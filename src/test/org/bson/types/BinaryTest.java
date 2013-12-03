package org.bson.types;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class BinaryTest {

    Binary first = new Binary((byte) 0, new byte[]{0, 1, 2});
    Binary second = new Binary((byte) 1, new byte[]{0, 1, 2});
    Binary third = new Binary((byte) 0, new byte[]{0, 1, 2, 3});
    Binary fourth = new Binary((byte) 0, new byte[]{0, 1, 2});

    @Test
    public void testEquals() throws Exception {
        assertFalse(first.equals(second));
        assertFalse(first.equals(third));
        assertEquals(first, fourth);
        assertFalse(first.equals("abc"));
        assertFalse(first.equals(null));
    }

    @Test
    public void testHashCode() throws Exception {
        assertTrue(first.hashCode() == fourth.hashCode());
        assertFalse(first.hashCode() == second.hashCode());
        assertFalse(first.hashCode() == third.hashCode());
        assertFalse(second.hashCode() == third.hashCode());

    }
}
