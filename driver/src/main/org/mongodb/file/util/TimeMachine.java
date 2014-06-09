package org.mongodb.file.util;

import java.util.Date;

/**
 * A 'simple" modification helper DSL for expiring files in mongoFS stores
 * 
 * @author David Buschman
 * 
 */
public final class TimeMachine {

    private long milliseconds;
    private long number;

    public static TimeMachine now() {

        return new TimeMachine(new Date().getTime());
    }

    public static TimeMachine from(final long start) {

        return new TimeMachine(start);
    }

    private TimeMachine(final long milliseconds) {

        this.milliseconds = milliseconds;
    }

    public TimeMachine forward(final int number) {

        this.number = number;
        return this;
    }

    public TimeMachine backward(final int number) {

        this.number = 0 - number;
        return this;
    }

    public Date toDate() {

        return inTime();
    }

    public Date inTime() {

        return new Date(milliseconds);
    }

    public TimeMachine seconds() {

        this.milliseconds += (number * 1000);
        number = 0;
        return this;
    }

    public TimeMachine minutes() {

        this.milliseconds += (number * 60 * 1000);
        number = 0;
        return this;
    }

    public TimeMachine hours() {

        this.milliseconds += (number * 60 * 60 * 1000);
        number = 0;
        return this;
    }

    public TimeMachine days() {

        this.milliseconds += (number * 24 * 60 * 60 * 1000);
        number = 0;
        return this;
    }

    public TimeMachine years() {

        this.milliseconds += (number * 365L * 24 * 60 * 60 * 1000);
        number = 0;
        return this;
    }

}
