/**
 * 
 */
package com.google.code.morphia.logging.jdk;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.google.code.morphia.logging.Logr;
import com.google.code.morphia.logging.jdk.FasterJDKLogger;
import com.google.code.morphia.logging.jdk.FastestJDKLogger;
import com.google.code.morphia.logging.jdk.JDKLogger;

/**
 * @author doc
 */
public class JDKLoggerBench implements I
{
    private static Logr logger;
    private static JDKLoggerBench r = new JDKLoggerBench();

    public static void main(final String[] args) throws Exception
    {

        System.err.println("When Log-Level is beyond interest\n---------------------------------------------");
        Logger.getLogger(JDKLoggerBench.class.getName()).setLevel(Level.WARNING);
        System.setOut(new PrintStream(new OutputStream()
        {

            @Override
            public void write(final int b) throws IOException
            {

            }
        }));

        logger = new JDKLogger(JDKLoggerBench.class);
        bench();
        logger = new FasterJDKLogger(JDKLoggerBench.class);
        bench();
        logger = new FastestJDKLogger(JDKLoggerBench.class);
        bench();

        System.err.println("\nWhen Log-Level is within interest\n---------------------------------------------");
        Logger.getLogger(JDKLoggerBench.class.getName()).setLevel(Level.FINEST);

        logger = new JDKLogger(JDKLoggerBench.class);
        bench();
        logger = new FasterJDKLogger(JDKLoggerBench.class);
        bench();
        logger = new FastestJDKLogger(JDKLoggerBench.class);
        bench();

    }

    private static void calm()
    {
        System.gc();
        Thread.yield();
        try
        {
            Thread.sleep(5000);
        }
        catch (final InterruptedException e)
        {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    private static void bench()
    {
        long start;
        long end;
        System.err.println("\nUsing " + logger.getClass());
        System.err.println("Warmup");
        for (int j = 0; j < 10000; j++)
        {
            r.foo();
        }
        calm();

        System.err.println("Start");
        start = System.currentTimeMillis();
        for (int j = 0; j < 1000000; j++)
        {
            r.foo();
        }
        end = System.currentTimeMillis();
        System.err.println("RT " + ((end - start)) + "ms");
    }

    public void foo()
    {
        JDKLoggerBench.logger.debug("buh");
    }

}

interface I
{
    void foo();
}