/*
 * Copyright (c) 2008 - 2013 10gen, Inc. <http://10gen.com>
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

// CommandResult.java



package com.mongodb;


import static com.mongodb.DBObjects.toDBObject;

/**
 * A simple wrapper for the result of getLastError() calls and other commands
 */
public class CommandResult extends BasicDBObject {
    private static final long serialVersionUID = 5907909423864204060L;
    private final DBObject cmd;
    private final ServerAddress host;

    CommandResult(final DBObject cmd, final ServerAddress srv) {
        if (srv == null) {
            throw new IllegalArgumentException("server address is null");
        }
        this.cmd = cmd;
        host = srv;
        //so it is shown in toString/debug
        putAll(cmd);
        put("serverUsed", srv.toString());
    }

    CommandResult(final org.mongodb.CommandResult commandResult) {
        this(toDBObject(commandResult.getResponse()), new ServerAddress(commandResult.getAddress()));
    }

    /**
     * gets the "ok" field which is the result of the command
     *
     * @return True if ok
     */
    public boolean ok() {
        final Object o = get("ok");
        if (o == null) {
            throw new IllegalArgumentException("'ok' should never be null...");
        }

        if (o instanceof Boolean) {
            return (Boolean) o;
        }

        if (o instanceof Number) {
            return ((Number) o).intValue() == 1;
        }

        throw new IllegalArgumentException("can't figure out what to do with: " + o.getClass().getName());
    }

    /**
     * gets the "errmsg" field which holds the error message
     *
     * @return The error message or null
     */
    public String getErrorMessage() {
        final Object foo = get("errmsg");
        if (foo == null) {
            return null;
        }
        return foo.toString();
    }

    /**
     * utility method to create an exception with the command name
     *
     * @return The mongo exception or null
     */
    public MongoException getException() {
        if (!ok()) {
            final StringBuilder buf = new StringBuilder();

            final String cmdName;
            if (cmd != null) {
                //TODO this is not correct to get first element from set
                cmdName = cmd.keySet().iterator().next();
                buf.append("command failed [").append(cmdName).append("]: ");
            }
            else {
                buf.append("operation failed: ");
            }

            buf.append(toString());

            return new CommandFailureException(this, buf.toString());
        } else if (hasErr()) { // getLastError check
            return createException(getCode(), get("err").toString());
        }

        //all good, should never get here.
        return null;
    }

    private MongoException createException(final int code, final String message) {
        switch (code) {
            case 11000:
            case 11001:
            case 12582:
                return new MongoDuplicateKeyException(this);
            default:
                return new MongoException(code, message);
        }
    }

    /**
     * returns the "code" field, as an int
     *
     * @return -1 if there is no code
     */
    private int getCode() {
        int code = -1;
        if (get("code") instanceof Number) {
            code = ((Number) get("code")).intValue();
        }
        return code;
    }

    /**
     * check the "err" field
     *
     * @return if it has it, and isn't null
     */
    boolean hasErr() {
        final Object o = get("err");
        return (o != null && ((String) o).length() > 0);
    }

    /**
     * throws an exception containing the cmd name, in case the command failed, or the "err/code" information
     *
     * @throws MongoException
     */
    public void throwOnError() {
        if (!ok() || hasErr()) {
            throw getException();
        }
    }

    public ServerAddress getServerUsed() {
        return host;
    }
}
