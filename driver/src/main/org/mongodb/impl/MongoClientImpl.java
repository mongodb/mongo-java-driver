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

package org.mongodb.impl;

import org.mongodb.ClientAdmin;
import org.mongodb.MongoClient;
import org.mongodb.MongoClientOptions;
import org.mongodb.MongoDatabase;
import org.mongodb.MongoDatabaseOptions;
import org.mongodb.MongoServerBinding;
import org.mongodb.ServerAddress;
import org.mongodb.codecs.PrimitiveCodecs;
import org.mongodb.io.BufferPool;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

public class MongoClientImpl implements MongoClient {

    private final MongoServerBinding binding;
    private final MongoClientOptions clientOptions;
    private PrimitiveCodecs primitiveCodecs = PrimitiveCodecs.createDefault();
    private final ThreadLocal<MongoServerBinding> pinnedBinding = new ThreadLocal<MongoServerBinding>();

    public MongoClientImpl(final MongoClientOptions clientOptions, final MongoServerBinding binding) {
        this.clientOptions = clientOptions;
        this.binding = binding;
    }

    @Override
    public MongoDatabase getDatabase(final String databaseName) {
        return getDatabase(databaseName, MongoDatabaseOptions.builder().build());
    }

    @Override
    public MongoDatabase getDatabase(final String databaseName, final MongoDatabaseOptions options) {
        return new MongoDatabaseImpl(databaseName, this, options.withDefaults(clientOptions));
    }

    @Override
    public void withConnection(final Runnable runnable) {
        pinBinding();
        try {
            runnable.run();
        } finally {
            unpinBinding();
        }
    }

    @Override
    public <T> T withConnection(final Callable<T> callable) throws ExecutionException {
        pinBinding();
        try {
            return callable.call();
        } catch (Exception e) {
            throw new ExecutionException(e);
        } finally {
            unpinBinding();
        }
    }

    @Override
    public void close() {
        binding.close();
    }

    @Override
    public MongoClientOptions getOptions() {
        return clientOptions;
    }

    @Override
    public ClientAdmin tools() {
        return new ClientAdminImpl(getBinding(), primitiveCodecs);
    }

    @Override
    public List<ServerAddress> getServerAddressList() {
        return binding.getAllServerAddresses();
    }

    public MongoServerBinding getBinding() {
        if (pinnedBinding.get() != null) {
            return pinnedBinding.get();
        }
        return binding;
    }

    public BufferPool<ByteBuffer> getBufferPool() {
        return binding.getBufferPool();
    }

    private void pinBinding() {
        if (pinnedBinding.get() != null) {
            throw new IllegalStateException();
        }
        pinnedBinding.set(new MonotonicallyConsistentMongoServerBinding(binding));
    }

    private void unpinBinding() {
        MongoServerBinding bindingToUnpin = this.pinnedBinding.get();
        this.pinnedBinding.remove();
        bindingToUnpin.close();
    }
}
