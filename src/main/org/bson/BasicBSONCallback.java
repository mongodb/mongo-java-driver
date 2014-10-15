/*
 * Copyright (c) 2008-2014 MongoDB, Inc.
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

/**
 * An implementation of {@code BsonCallback} that creates an instance of BSONObject.
 */
public class BasicBSONCallback implements BSONCallback {

    /**
     * Creates a new instance.
     */
    public BasicBSONCallback(){
        reset();
    }

    /**
     * Factory method for creating a new BSONObject.
     *
     * @return a new BasicBSONObject.
     */
    public BSONObject create(){
        return new BasicBSONObject();
    }

    /**
     * Factory method for creating a new BSON List.
     *
     * @return a new BasicBSONList.
     */
    protected BSONObject createList() {
        return new BasicBSONList();
    }

    @Override
    public BSONCallback createBSONCallback(){
        return new BasicBSONCallback();
    }

    /**
     * Helper method to create either a BSON Object or a BSON List depending upon whether the {@code array} parameter is true or not.
     *
     * @param array set to true to create a new BSON List, otherwise will create a new BSONObject
     * @param path  a list of field names to navigate to this field in the document
     * @return the new BSONObject
     */
    public BSONObject create(final boolean array, final List<String> path) {
        if ( array )
            return createList();
        return create();
    }

    @Override
    public void objectStart(){
        if ( _stack.size() > 0 )
	        throw new IllegalStateException( "something is wrong" );

	    objectStart(false);
    }

    /**
     * @deprecated instead, use {@link #arrayStart()} if {@code array} is true, and {@link #objectStart()} if if {@code array} is false 
     */
    @Deprecated
    public void objectStart(boolean array){
        _root = create(array, null);
        _stack.add( (BSONObject)_root );
    }

    @Override
    public void objectStart(final String name) {
        objectStart( false , name );
    }

    /**
     * @deprecated instead, use {@link #arrayStart(String)} if {@code array} is true, and {@link #objectStart(String)} if {@code array}
     * is false
     */
    @Deprecated
    public void objectStart(boolean array, String name){
        _nameStack.addLast( name );
        final BSONObject o = create( array , _nameStack );
        _stack.getLast().put( name , o);
        _stack.addLast( o );
    }

    @Override
    public Object objectDone(){
        final BSONObject o =_stack.removeLast();
        if ( _nameStack.size() > 0 )
            _nameStack.removeLast();
        else if ( _stack.size() > 0 )
	        throw new IllegalStateException( "something is wrong" );

        return !BSON.hasDecodeHooks() ? o : (BSONObject)BSON.applyDecodingHooks(o);
    }

    @Override
    public void arrayStart() {
        objectStart(true);
    }

    @Override
    public void arrayStart(final String name) {
        objectStart( true , name );
    }

    @Override
    public Object arrayDone(){
        return objectDone();
    }

    @Override
    public void gotNull(final String name) {
        cur().put( name , null );
    }

    @Override
    public void gotUndefined(final String name) {
    }

    @Override
    public void gotMinKey(final String name) {
        cur().put( name , new MinKey() );
    }

    @Override
    public void gotMaxKey(final String name) {
        cur().put( name , new MaxKey() );
    }

    @Override
    public void gotBoolean(final String name, final boolean value) {
        _put(name, value);
    }

    @Override
    public void gotDouble(final String name, final double value) {
        _put(name, value);
    }

    @Override
    public void gotInt(final String name, final int value) {
        _put(name, value);
    }

    @Override
    public void gotLong(final String name, final long value) {
        _put(name, value);
    }

    @Override
    public void gotDate(final String name, final long millis) {
        _put( name , new Date( millis ) );
    }

    @Override
    public void gotRegex(final String name, final String pattern, final String flags) {
        _put( name , Pattern.compile( pattern , BSON.regexFlags( flags ) ) );
    }

    @Override
    public void gotString(final String name, final String value) {
        _put(name, value);
    }

    @Override
    public void gotSymbol(final String name, final String value) {
        _put(name, value);
    }

    @Override
    public void gotTimestamp( String name , int time , int inc ){
        _put( name , new BSONTimestamp( time , inc ) );
    }

    @Override
    public void gotObjectId(final String name, final ObjectId id) {
        _put( name , id );
    }

    @Override
    public void gotDBRef( String name , String ns , ObjectId id ){
        _put( name , new BasicBSONObject( "$ns" , ns ).append( "$id" , id ) );
    }

    @Deprecated
    @Override
    public void gotBinaryArray(final String name, final byte[] data) {
        gotBinary( name, BSON.B_GENERAL, data );
    }

    @Override
    public void gotBinary(final String name, final byte type, final byte[] data) {
        if( type == BSON.B_GENERAL || type == BSON.B_BINARY )
            _put( name , data );
        else
            _put( name , new Binary( type , data ) );
    }

    @Override
    public void gotUUID(final String name, final long part1, final long part2) {
        _put( name , new UUID(part1, part2) );
    }

    @Override
    public void gotCode(final String name, final String code) {
        _put( name , new Code( code ) );
    }

    @Override
    public void gotCodeWScope(final String name, final String code, final Object scope) {
        _put( name , new CodeWScope( code, (BSONObject)scope ) );
    }

    /**
     * Puts a new value into the document.
     *
     * @param name the name of the field
     * @param o    the value
     */
    protected void _put(final String name, final Object o) {
        cur().put(name, !BSON.hasDecodeHooks() ? o : BSON.applyDecodingHooks(o));
    }

    /**
     * Gets the current value
     *
     * @return the current value
     */
    protected BSONObject cur(){
        return _stack.getLast();
    }

    /**
     * Gets the name of the current field
     *
     * @return the name of the current field.
     */
    protected String curName(){
        return (!_nameStack.isEmpty()) ? _nameStack.getLast() : null;
    }

    @Override
    public Object get(){
	    return _root;
    }

    /**
     * Sets the root document for this position
     *
     * @param o the new root document
     */
    protected void setRoot(Object o) {
	    _root = o;
    }

    /**
     * Returns whether this is the top level or not
     *
     * @return true if there's nothing on the stack, and this is the top level of the document.
     */
    protected boolean isStackEmpty() {
	    return _stack.size() < 1;
    }

    @Override
    public void reset(){
        _root = null;
        _stack.clear();
        _nameStack.clear();
    }

    private Object _root;
    private final LinkedList<BSONObject> _stack = new LinkedList<BSONObject>();
    private final LinkedList<String> _nameStack = new LinkedList<String>();
}
