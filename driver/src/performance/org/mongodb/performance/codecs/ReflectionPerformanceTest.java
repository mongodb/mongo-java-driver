package org.mongodb.performance.codecs;

import org.junit.Test;

import java.lang.reflect.Field;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.lang.String.format;
import static org.mongodb.performance.codecs.PerfTestUtils.NUMBER_OF_NANO_SECONDS_IN_A_SECOND;
import static org.mongodb.performance.codecs.PerfTestUtils.calculateOperationsPerSecond;
import static org.mongodb.performance.codecs.PerfTestUtils.testCleanup;

//CHECKSTYLE:OFF
public class ReflectionPerformanceTest {
    private static final int NUMBER_OF_TIMES_FOR_WARMUP = 10000;
    private static final int NUMBER_OF_TIMES_TO_RUN = 100000000;

    //This field is used via reflection, since this class is, in fact, testing reflection
    @SuppressWarnings("UnusedDeclaration")
    private List<Integer> listField;

    @Test
    public void outputPerformanceForGetDeclaredFields() throws NoSuchFieldException, InterruptedException {
        // 1,366,496 ops per second

        Field[] fields = null;
        for (int i = 0; i < NUMBER_OF_TIMES_FOR_WARMUP; i++) {
            fields = this.getClass().getDeclaredFields();
        }

        for (int i = 0; i < 3; i++) {
            long startTime = System.nanoTime();
            for (int j = 0; j < NUMBER_OF_TIMES_TO_RUN; j++) {
                fields = this.getClass().getDeclaredFields();
            }
            long endTime = System.nanoTime();

            outputResults(startTime, endTime);
            System.out.println(Arrays.toString(fields));
            testCleanup();
        }
    }

    @Test
    public void outputPerformanceForGetDeclaredField() throws NoSuchFieldException, InterruptedException {
        // 1,161,143 ops per second

        Field field = null;
        for (int i = 0; i < NUMBER_OF_TIMES_FOR_WARMUP; i++) {
            field = this.getClass().getDeclaredField("listField");
        }

        for (int i = 0; i < 3; i++) {
            long startTime = System.nanoTime();
            for (int j = 0; j < NUMBER_OF_TIMES_TO_RUN; j++) {
                field = this.getClass().getDeclaredField("listField");
            }
            long endTime = System.nanoTime();

            outputResults(startTime, endTime);
            System.out.println(field);
            testCleanup();
        }
    }

    @Test
    public void outputPerformanceForGetDeclaredFieldsFollowedByGetDeclaredField() throws NoSuchFieldException, InterruptedException {
        //testing this to see if there's simply a one-off overhead, or if it hits every call

        // 706,178 ops per second - nearly half the performance of just a single call

        Field[] fields = null;
        Field field = null;
        for (int i = 0; i < NUMBER_OF_TIMES_FOR_WARMUP; i++) {
            fields = this.getClass().getDeclaredFields();
            field = this.getClass().getDeclaredField("listField");
        }

        for (int i = 0; i < 3; i++) {
            long startTime = System.nanoTime();
            for (int j = 0; j < NUMBER_OF_TIMES_TO_RUN; j++) {
                fields = this.getClass().getDeclaredFields();
                field = this.getClass().getDeclaredField("listField");
            }
            long endTime = System.nanoTime();

            outputResults(startTime, endTime);
            //this prevents the call being optimised away
            System.out.println(Arrays.toString(fields));
            System.out.println(field);
            testCleanup();
        }
    }

    @Test
    public void outputPerformanceForGetType() throws NoSuchFieldException, InterruptedException {
        // 32,509,752,926 ops per second, and it eventually gets optimised away

        Field fieldOnPojo = this.getClass().getDeclaredField("listField");
        Class<?> classOfIterable = null;
        for (int i = 0; i < NUMBER_OF_TIMES_FOR_WARMUP; i++) {
            classOfIterable = fieldOnPojo.getType();
        }

        for (int i = 0; i < 3; i++) {
            long startTime = System.nanoTime();
            for (int j = 0; j < NUMBER_OF_TIMES_TO_RUN; j++) {
                classOfIterable = fieldOnPojo.getType();
            }
            long endTime = System.nanoTime();

            outputResults(startTime, endTime);
            System.out.println(classOfIterable);
            testCleanup();
        }
    }

    @Test
    public void outputPerformanceForIsAssignableFromWhenFalse() throws NoSuchFieldException, InterruptedException {
        // 10,242,753,252 ops per second first run
        // 40,569,038 ops per second 2nd and 3rd.  Huh??
        // So, slower than getType, quicker than getDeclaredField

        Field fieldOnPojo = this.getClass().getDeclaredField("listField");
        Class<?> classOfIterable = fieldOnPojo.getType();
        boolean isSomeSortOfSet = true;
        for (int i = 0; i < NUMBER_OF_TIMES_FOR_WARMUP; i++) {
            isSomeSortOfSet = Set.class.isAssignableFrom(classOfIterable);
        }

        for (int i = 0; i < 3; i++) {
            long startTime = System.nanoTime();
            for (int j = 0; j < NUMBER_OF_TIMES_TO_RUN; j++) {
                isSomeSortOfSet = Set.class.isAssignableFrom(classOfIterable);
            }
            long endTime = System.nanoTime();

            outputResults(startTime, endTime);
            System.out.println(isSomeSortOfSet);
            testCleanup();
        }
    }

    @Test
    public void outputPerformanceForIsAssignableFromWhenTrue() throws NoSuchFieldException, InterruptedException {
        // 12,963,443,090 ops per second first run
        // 649,557,327 ops per second 2nd and 3rd.  Huh??
        // So marginally faster if it is assignable

        Field fieldOnPojo = this.getClass().getDeclaredField("listField");
        Class<?> classOfIterable = fieldOnPojo.getType();
        boolean isSomeSortOfSet = true;
        for (int i = 0; i < NUMBER_OF_TIMES_FOR_WARMUP; i++) {
            isSomeSortOfSet = List.class.isAssignableFrom(classOfIterable);
        }

        for (int i = 0; i < 3; i++) {
            long startTime = System.nanoTime();
            for (int j = 0; j < NUMBER_OF_TIMES_TO_RUN; j++) {
                isSomeSortOfSet = List.class.isAssignableFrom(classOfIterable);
            }
            long endTime = System.nanoTime();

            outputResults(startTime, endTime);
            System.out.println(isSomeSortOfSet);
            testCleanup();
        }
    }

    @Test
    public void outputPerformanceForGetGenericType() throws NoSuchFieldException, InterruptedException {
        // 17,543,859,649 ops per second
        // and it eventually gets optimised away

        Field fieldOnPojo = this.getClass().getDeclaredField("listField");
        Type genericType = null;
        for (int i = 0; i < NUMBER_OF_TIMES_FOR_WARMUP; i++) {
            genericType = fieldOnPojo.getGenericType();
        }

        for (int i = 0; i < 3; i++) {
            long startTime = System.nanoTime();
            for (int j = 0; j < NUMBER_OF_TIMES_TO_RUN; j++) {
                genericType = fieldOnPojo.getGenericType();
            }
            long endTime = System.nanoTime();

            outputResults(startTime, endTime);
            System.out.println(genericType);
            testCleanup();
        }
    }

    @Test
    public void outputPerformanceForGetActualTypeArguments() throws NoSuchFieldException, InterruptedException {
        // 46,265,324 ops per second
        // 101,162,867 ops per second on 2nd and 3rd times

        Field fieldOnPojo = this.getClass().getDeclaredField("listField");
        Type[] actualTypeArguments = null;
        for (int i = 0; i < NUMBER_OF_TIMES_FOR_WARMUP; i++) {
            actualTypeArguments = ((ParameterizedType) fieldOnPojo.getGenericType()).getActualTypeArguments();
        }

        for (int i = 0; i < 3; i++) {
            long startTime = System.nanoTime();
            for (int j = 0; j < NUMBER_OF_TIMES_TO_RUN; j++) {
                actualTypeArguments = ((ParameterizedType) fieldOnPojo.getGenericType()).getActualTypeArguments();
            }
            long endTime = System.nanoTime();

            outputResults(startTime, endTime);
            System.out.println(Arrays.toString(actualTypeArguments));
            testCleanup();
        }
    }

    @Test
    public void outputPerformanceForSetAccessible() throws NoSuchFieldException, InterruptedException {
        Field fieldOnPojo = this.getClass().getDeclaredField("listField");
        // 1,884,765,441 ops per second (same for false)
        for (int i = 0; i < NUMBER_OF_TIMES_FOR_WARMUP; i++) {
            fieldOnPojo.setAccessible(true);
        }

        for (int i = 0; i < 3; i++) {
            long startTime = System.nanoTime();
            for (int j = 0; j < NUMBER_OF_TIMES_TO_RUN; j++) {
                fieldOnPojo.setAccessible(true);
            }
            long endTime = System.nanoTime();

            outputResults(startTime, endTime);
            testCleanup();
        }

    }

    @Test
    public void outputPerformanceForGetValue() throws Exception {
        Field fieldOnPojo = this.getClass().getDeclaredField("listField");
        // 239,823,682 ops per second
        Object fieldValue = null;
        for (int i = 0; i < NUMBER_OF_TIMES_FOR_WARMUP; i++) {
            fieldValue = fieldOnPojo.get(this);
        }

        for (int i = 0; i < 3; i++) {
            long startTime = System.nanoTime();
            for (int j = 0; j < NUMBER_OF_TIMES_TO_RUN; j++) {
                fieldValue = fieldOnPojo.get(this);
            }
            long endTime = System.nanoTime();

            outputResults(startTime, endTime);
            System.out.println(fieldValue);
            testCleanup();
        }
    }

    @Test
    public void outputMatcherResults() throws Exception {
        String fieldName = "theFieldName";
        // 1,158,292 ops per second using String matches
        // 1,045,351 ops per second using Pattern matches (new pattern every time)
        // 3,655,348 ops per second when you compile the pattern
        boolean validFieldName = false;
        Pattern pattern = Pattern.compile("([a-zA-Z_][\\w$]*)");

        for (int i = 0; i < NUMBER_OF_TIMES_FOR_WARMUP; i++) {
            validFieldName = isValidFieldName(fieldName, pattern);
        }

        for (int i = 0; i < 3; i++) {
            long startTime = System.nanoTime();
            for (int j = 0; j < NUMBER_OF_TIMES_TO_RUN; j++) {
                validFieldName = isValidFieldName(fieldName, pattern);
            }
            long endTime = System.nanoTime();

            outputResults(startTime, endTime);
            System.out.println(validFieldName);
            testCleanup();
        }
    }

    private boolean isValidFieldName(final String fieldName, final Pattern pattern) {
        //We need to document that fields starting with a $ will be ignored
        //and we probably need to be able to either disable this validation or make it pluggable
        Matcher matcher = pattern.matcher(fieldName);
        return matcher.matches();
    }

    private void outputResults(final long startTime, final long endTime) {
        long timeTakenInNanos = endTime - startTime;
        System.out.println(format("Test took: %,d ns", timeTakenInNanos));
        System.out.println(format("Test took: %,.3f seconds", timeTakenInNanos / NUMBER_OF_NANO_SECONDS_IN_A_SECOND));
        System.out.println(format("%,.0f ops per second%n", calculateOperationsPerSecond(timeTakenInNanos, NUMBER_OF_TIMES_TO_RUN)));
    }
    //CHECKSTYLE:ON
}
