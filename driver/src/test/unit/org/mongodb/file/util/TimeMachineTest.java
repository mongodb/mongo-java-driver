package org.mongodb.file.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class TimeMachineTest {

    @Test
    public void testSeconds() {

        assertEquals(3000, TimeMachine.from(0).forward(3).seconds().inTime().getTime());
        assertEquals(-3000, TimeMachine.from(0).backward(3).seconds().inTime().getTime());
    }

    @Test
    public void testMinutes() {

        assertEquals(3 * 60 * 1000, TimeMachine.from(0).forward(3).minutes().inTime().getTime());
        assertEquals(-3 * 60 * 1000, TimeMachine.from(0).backward(3).minutes().inTime().getTime());
    }

    @Test
    public void testHours() {

        assertEquals(3 * 60 * 60 * 1000, TimeMachine.from(0).forward(3).hours().inTime().getTime());
        assertEquals(-3 * 60 * 60 * 1000, TimeMachine.from(0).backward(3).hours().inTime().getTime());
    }

    @Test
    public void testDays() {

        assertEquals(3 * 24 * 60 * 60 * 1000, TimeMachine.from(0).forward(3).days().inTime().getTime());
        assertEquals(-3 * 24 * 60 * 60 * 1000, TimeMachine.from(0).backward(3).days().inTime().getTime());
    }

    @Test
    public void testYears() {

        assertEquals(3 * 365L * 24 * 60 * 60 * 1000, TimeMachine.from(0).forward(3).years().inTime().getTime());
        assertEquals(-3 * 365L * 24 * 60 * 60 * 1000, TimeMachine.from(0).backward(3).years().inTime().getTime());
    }

    @Test
    public void testNow() {

        assertTrue(System.currentTimeMillis() < TimeMachine.now().forward(1).seconds().inTime().getTime());
        assertTrue(System.currentTimeMillis() > TimeMachine.now().backward(1).seconds().inTime().getTime());
    }
}
