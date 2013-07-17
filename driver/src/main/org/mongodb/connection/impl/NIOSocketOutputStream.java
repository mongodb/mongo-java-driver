/**
 * Copyright [2012] [Gihan Munasinghe ayeshka@gmail.com ]
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */


package org.mongodb.connection.impl;


import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


public class NIOSocketOutputStream extends OutputStream {

    private final ByteArrayOutputStream holder;
    private final SocketClient client;
    private boolean streamClosed = false;
    private Boolean isWriteWait = false;

    private final Lock writeLock = new ReentrantLock();
    private final Condition cond = writeLock.newCondition();

    private static final int MAX_BUFF_SIZE = 1024;

    NIOSocketOutputStream(final SocketClient cli) {
        holder = new ByteArrayOutputStream();
        client = cli;
    }

    @Override
    public void write(final int b) throws IOException {
        if (streamClosed) {
            throw new IOException("Write stream closed");
        }
        synchronized (holder) {
            holder.write(b);
            flush();
        }

    }

    @Override
    public void write(final byte[] arg0) throws IOException {
        if (streamClosed) {
            throw new IOException("Write stream closed");
        }
        synchronized (holder) {
            holder.write(arg0);
            flush();
        }

    }

    @Override
    public void write(final byte[] b, final int off, final int len) throws IOException {
        if (streamClosed) {
            throw new IOException("Write stream closed");
        }
        synchronized (holder) {
            holder.write(b, off, len);
            flush();
        }

    }

    @Override
    public void flush() throws IOException {

        if (streamClosed) {
            throw new IOException("Write stream closed");
        }
        try {
            writeLock.lock();
            synchronized (isWriteWait) {
                isWriteWait = true;
            }
            client.triggerWrite();
            while (isWriteWait) {
                cond.await();
            }

            client.doWrite();
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        } finally {
            writeLock.unlock();
        }

    }

    protected void notifyWrite() {
        try {
            writeLock.lock();
            synchronized (isWriteWait) {
                isWriteWait = false;
            }
            cond.signal();
        } finally {
            writeLock.unlock();
        }

    }


    protected ByteBuffer getByteBuffer() {
        ByteBuffer buff = null;
        synchronized (holder) {
            buff = ByteBuffer.wrap(holder.toByteArray());
            holder.reset();
        }
        return buff;
    }

    @Override
    public void close() throws IOException {
        if (!streamClosed) {
            streamClosed = true;
        }

    }

    protected boolean isWriteWait() {
        return isWriteWait;
    }
}
