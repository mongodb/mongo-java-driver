/*
 * Copyright (c) 2008 - 2012 10gen, Inc. <http://10gen.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.code.morphia.testutil;

public abstract class AssertedFailure {
    private Throwable exceptionRaised;
    private Class<? extends Throwable> expected;

    protected AssertedFailure(final Class<? extends Throwable> expectedAssertionType) {
        expected = expectedAssertionType;
        run();
    }

    protected AssertedFailure() {
        run();
    }

    private void run() {
        try {
            thisMustFail();
            throw new AssertedFailureDidNotHappenException();
        } catch (Throwable e) {
            if (e instanceof AssertedFailureDidNotHappenException) {
                final AssertedFailureDidNotHappenException assertedOne = (AssertedFailureDidNotHappenException) e;
                throw assertedOne;
            }

            exceptionRaised = e;
            if (dumpToSystemOut()) {
                System.out.println("AssertedFailure:" + exceptionRaised);
            }
            if (expected != null) {
                final Throwable ex = getWrappedException(e, expected);
                if (ex == null) {
                    throw new AssertedFailureDidNotHappenException("unexpected exception class. got '"
                                                                   + e.getClass().getName() + "' instead of "
                                                                   + "expected '" + expected.getName() + "'",
                                                                  e);
                }

            }
        }
    }

    @SuppressWarnings("unchecked")
    public static <T extends Throwable> T getWrappedException(final Throwable throwable, final Class<T> targetClass) {
        if (throwable == null) {
            return null;
        }

        if (throwable.getClass() == targetClass) {
            return (T) throwable;
        }
        else {
            return getWrappedException(throwable.getCause(), targetClass);
        }
    }

    protected abstract void thisMustFail();

    protected boolean dumpToSystemOut() {
        return false;
    }

    public Throwable getExceptionRaised() {
        return exceptionRaised;
    }

}

class AssertedFailureDidNotHappenException extends RuntimeException {

    public AssertedFailureDidNotHappenException(final String string, final Throwable e) {
        super(string, e);
    }

    public AssertedFailureDidNotHappenException() {
        super();
    }

    private static final long serialVersionUID = 1L;
}
