/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the 'License');
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an 'AS IS' BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.connection.netty

import io.netty.buffer.ByteBufAllocator
import org.bson.ByteBufNIO
import spock.lang.Specification

import java.nio.ByteBuffer

class ByteBufSpecification extends Specification {
    def 'should set position and limit correctly'() {
        expect:
        buf.capacity() == 16
        buf.position() == 0
        buf.limit() == 16

        when:
        buf.put(new byte[10], 0, 10)

        then:
        buf.position() == 10
        buf.limit() == 16

        when:
        buf.flip()

        then:
        buf.position() == 0
        buf.limit() == 10

        when:
        buf.position(3)

        then:
        buf.position() == 3
        buf.limit() == 10

        when:
        buf.limit(7)

        then:
        buf.position() == 3
        buf.limit() == 7

        when:
        buf.get(new byte[4])

        then:
        buf.position() == 7
        buf.limit() == 7

        where:
        buf << [new ByteBufNIO(ByteBuffer.allocate(16)),
                new NettyByteBuf(ByteBufAllocator.DEFAULT.buffer(16))
        ]
    }

    // the fact that setting the limit on a NettyByteBuf throws an exception is a design flaw in the ByteBuf interface, but one that
    // doesn't need to be addressed immediately, as the driver does not need to be able to set the limit while writing to a the buffer,
    // only when reading.
    def 'should throw when setting limit while writing'() {
        given:
        def buf = new NettyByteBuf(ByteBufAllocator.DEFAULT.buffer(16))

        when:
        buf.limit(10)

        then:
        thrown(UnsupportedOperationException)
    }

    def 'should manage reference count of proxied Netty ByteBuf correctly'() {
        given:
        def nettyBuf = ByteBufAllocator.DEFAULT.buffer(16)

        when:
        def buf = new NettyByteBuf(nettyBuf)

        then:
        nettyBuf.refCnt() == 1

        when:
        buf.retain()

        then:
        nettyBuf.refCnt() == 2

        when:
        buf.release()

        then:
        nettyBuf.refCnt() == 1

        when:
        buf.release()

        then:
        nettyBuf.refCnt() == 0
    }

    def 'should manage reference count of duplicated proxied Netty ByteBuf correctly'() {
        given:
        def nettyBuf = ByteBufAllocator.DEFAULT.buffer(16)
        def buf = new NettyByteBuf(nettyBuf)

        when:
        def duplicated = buf.duplicate()

        then:
        nettyBuf.refCnt() == 2

        when:
        buf.retain()

        then:
        nettyBuf.refCnt() == 3

        when:
        buf.release()

        then:
        nettyBuf.refCnt() == 2

        when:
        duplicated.release()

        then:
        nettyBuf.refCnt() == 1

        when:
        buf.release()

        then:
        nettyBuf.refCnt() == 0
    }
}
