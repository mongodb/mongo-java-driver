/**
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
 *
 */

package org.mongodb;

class MongoServer {
//    private SimplePool<MongoConnection> pool;
//    private final ServerAddress address;
//    private final BufferPool bufferPool;
//
//    MongoServer(ServerAddress address, BufferPool bufferPool, int poolSize) {
//        this.address = address;
//        this.bufferPool = bufferPool;
//        // TODO: This needs to be it's own class with JMX, etc
//        pool = new SimplePool<MongoConnection>(address.toString(), poolSize) {
//            @Override
//            protected MongoConnection createNew() {
//                return new MongoConnection(MongoServer.this.address, MongoServer.this.bufferPool);
//            }
//        };
//    }
//
//     <T> MongoReplyMessage<T> sendMessage(MongoRequestMessage requestMessage, WriteConcern writeConcern) {
//         try {
//             MongoConnection mongoConnection = pool.get();
//             try {
//                mongoConnection.sendMessage(requestMessage);
//             } catch (IOException e) {
//                 throw new MongoException("Unable to send message", e);
//             } finally {
//                 pool.done(mongoConnection);
//             }
//         } catch (InterruptedException e) {
//             throw new MongoInterruptedException(e);
//         }
//         return null;
//     }
}
