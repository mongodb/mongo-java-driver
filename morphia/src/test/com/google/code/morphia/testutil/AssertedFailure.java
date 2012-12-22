package com.google.code.morphia.testutil;

/**
 * @author uwe
 */
public abstract class AssertedFailure
{
    private Throwable exceptionRaised;
    private Class<? extends Throwable> expected;

    protected AssertedFailure(final Class<? extends Throwable> expectedAssertionType)
    {
        expected = expectedAssertionType;
        run();
    }

    protected AssertedFailure()
    {
        run();
    }

    private void run()
    {
        try
        {
            thisMustFail();
            throw new AssertedFailureDidNotHappenException();
        }
        catch (Throwable e)
        {
            if (e instanceof AssertedFailureDidNotHappenException)
            {
                AssertedFailureDidNotHappenException assertedOne = (AssertedFailureDidNotHappenException) e;
                throw assertedOne;
            }

            exceptionRaised = e;
            if (dumpToSystemOut())
            {
                System.out.println("AssertedFailure:" + exceptionRaised);
            }
            if (expected != null)
            {
                Throwable ex = getWrappedException(e, expected);
                if (ex == null)
                {
                    throw new AssertedFailureDidNotHappenException("unexpected exception class. got '"
                            + e.getClass().getName() + "' instead of expected '" + expected.getName() + "'", e);
                }

            }
        }
    }

    @SuppressWarnings("unchecked")
    public static <T extends Throwable> T getWrappedException(final Throwable throwable, final Class<T> targetClass)
    {
        if (throwable == null)
        {
            return null;
        }

        if (throwable.getClass() == targetClass)
        {
            return (T) throwable;
        }
        else
        {
            return getWrappedException(throwable.getCause(), targetClass);
        }
    }

    protected abstract void thisMustFail() throws Throwable;

    protected boolean dumpToSystemOut()
    {
        return false;
    }

    public Throwable getExceptionRaised()
    {
        return exceptionRaised;
    }

}

class AssertedFailureDidNotHappenException extends RuntimeException
{

    public AssertedFailureDidNotHappenException(final String string, final Throwable e)
    {
        super(string, e);
    }

    public AssertedFailureDidNotHappenException()
    {
        super();
    }

    private static final long serialVersionUID = 1L;
}
