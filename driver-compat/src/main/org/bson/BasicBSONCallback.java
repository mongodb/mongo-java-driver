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

// BasicBSONCallback.java

package org.bson;

import org.bson.types.BSONTimestamp;
import org.bson.types.BasicBSONList;
import org.bson.types.Binary;
import org.bson.types.Code;
import org.bson.types.CodeWScope;
import org.bson.types.MaxKey;
import org.bson.types.MinKey;
import org.bson.types.ObjectId;

import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;
import java.util.regex.Pattern;

public class BasicBSONCallback implements BSONCallback {

    public BasicBSONCallback() {
        reset();
    }

    public BSONObject create() {
        return new BasicBSONObject();
    }

    protected BSONObject createList() {
        return new BasicBSONList();
    }

    public BSONCallback createBSONCallback() {
        return new BasicBSONCallback();
    }

    public BSONObject create(final boolean array, final List<String> path) {
        if (array) {
            return createList();
        }
        return create();
    }

    public void objectStart() {
        if (stack.size() > 0) {
            throw new IllegalStateException("something is wrong");
        }

        objectStart(false);
    }

    public void objectStart(final boolean array) {
        root = create(array, null);
        stack.add((BSONObject) root);
    }

    public void objectStart(final String name) {
        objectStart(false, name);
    }

    public void objectStart(final boolean array, final String name) {
        nameStack.addLast(name);
        final BSONObject o = create(array, nameStack);
        stack.getLast().put(name, o);
        stack.addLast(o);
    }

    public Object objectDone() {
        final BSONObject o = stack.removeLast();
        if (nameStack.size() > 0) {
            nameStack.removeLast();
        } else if (stack.size() > 0) {
            throw new IllegalStateException("something is wrong");
        }

        return !BSON.hasDecodeHooks() ? o : (BSONObject) BSON.applyDecodingHooks(o);
    }

    public void arrayStart() {
        objectStart(true);
    }

    public void arrayStart(final String name) {
        objectStart(true, name);
    }

    public Object arrayDone() {
        return objectDone();
    }

    public void gotNull(final String name) {
        cur().put(name, null);
    }

    public void gotUndefined(final String name) {
    }

    public void gotMinKey(final String name) {
        cur().put(name, new MinKey());
    }

    public void gotMaxKey(final String name) {
        cur().put(name, new MaxKey());
    }

    public void gotBoolean(final String name, final boolean v) {
        _put(name, v);
    }

    public void gotDouble(final String name, final double v) {
        _put(name, v);
    }

    public void gotInt(final String name, final int v) {
        _put(name, v);
    }

    public void gotLong(final String name, final long v) {
        _put(name, v);
    }

    public void gotDate(final String name, final long millis) {
        _put(name, new Date(millis));
    }

    public void gotRegex(final String name, final String pattern, final String flags) {
        _put(name, Pattern.compile(pattern, BSON.regexFlags(flags)));
    }

    public void gotString(final String name, final String v) {
        _put(name, v);
    }

    public void gotSymbol(final String name, final String v) {
        _put(name, v);
    }

    public void gotTimestamp(final String name, final int time, final int inc) {
        _put(name, new BSONTimestamp(time, inc));
    }

    public void gotObjectId(final String name, final ObjectId id) {
        _put(name, id);
    }

    public void gotDBRef(final String name, final String ns, final ObjectId id) {
        _put(name, new BasicBSONObject("$ns", ns).append("$id", id));
    }

    @Deprecated
    public void gotBinaryArray(final String name, final byte[] data) {
        gotBinary(name, BSON.B_GENERAL, data);
    }

    public void gotBinary(final String name, final byte type, final byte[] data) {
        if (type == BSON.B_GENERAL || type == BSON.B_BINARY) {
            _put(name, data);
        } else {
            _put(name, new Binary(type, data));
        }
    }

    public void gotUUID(final String name, final long part1, final long part2) {
        _put(name, new UUID(part1, part2));
    }

    public void gotCode(final String name, final String code) {
        _put(name, new Code(code));
    }

    public void gotCodeWScope(final String name, final String code, final Object scope) {
        _put(name, new CodeWScope(code, (BSONObject) scope));
    }

    protected void _put(final String name, final Object o) {
        cur().put(name, !BSON.hasDecodeHooks() ? o : BSON.applyDecodingHooks(o));
    }

    protected BSONObject cur() {
        return stack.getLast();
    }

    protected String curName() {
        return (!nameStack.isEmpty()) ? nameStack.getLast() : null;
    }

    public Object get() {
        return root;
    }

    protected void setRoot(final Object o) {
        root = o;
    }

    protected boolean isStackEmpty() {
        return stack.size() < 1;
    }

    public void reset() {
        root = null;
        stack.clear();
        nameStack.clear();
    }

    private Object root;
    private final LinkedList<BSONObject> stack = new LinkedList<BSONObject>();
    private final LinkedList<String> nameStack = new LinkedList<String>();
}
