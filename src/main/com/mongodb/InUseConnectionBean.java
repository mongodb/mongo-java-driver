/**
 * Copyright (c) 2008 - 2011 10gen, Inc. <http://10gen.com>
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.mongodb;

import java.util.concurrent.TimeUnit;

/**
 * This class is NOT part of the public API.  Be prepared for non-binary compatible changes in minor releases.
 *
 * @deprecated This class will be removed in 3.x versions of the driver,
 *             so please remove it from your compile time dependencies.
 */
@Deprecated
public class InUseConnectionBean {

    InUseConnectionBean(final DBPort port, long currentNanoTime) {
        DBPort.ActiveState activeState = port.getActiveState();
        if (activeState == null) {
            durationMS = 0;
            namespace = null;
            opCode = null;
            query = null;
            threadName = null;
            numDocuments = 0;
        }
        else {
            durationMS = TimeUnit.NANOSECONDS.toMillis(currentNanoTime - activeState.startTime);
            namespace = activeState.outMessage.getNamespace();
            opCode = activeState.outMessage.getOpCode();
            query = activeState.outMessage.getQuery() != null ? activeState.outMessage.getQuery().toString() : null;
            threadName = activeState.threadName;
            numDocuments = activeState.outMessage.getNumDocuments();
        }
        localPort = port.getLocalPort();
    }

    public String getNamespace() {
        return namespace;
    }

    public OutMessage.OpCode getOpCode() {
        return opCode;
    }

    public String getQuery() {
        return query;
    }

    public int getLocalPort() {
        return localPort;
    }

    public long getDurationMS() {
        return durationMS;
    }

    public String getThreadName() {
        return threadName;
    }

    public int getNumDocuments() {
        return numDocuments;
    }

    private final String namespace;
    private final OutMessage.OpCode opCode;
    private final String query;
    private final int localPort;
    private final long durationMS;
    private final String threadName;
    private final int numDocuments;
}
