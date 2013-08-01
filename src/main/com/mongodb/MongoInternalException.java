// MongoInternalException.java

/**
 *      Copyright (C) 2008 10gen Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package com.mongodb;

/**
 * An Mongo exception internal to the driver, not carrying any error code
 * @author antoine
 */
public class MongoInternalException extends MongoException {

    private static final long serialVersionUID = -4415279469780082174L;

    /**
     *
     * @param msg the message
     */
    public MongoInternalException( String msg ){
        super( msg );
    }

    /**
     *
     * @param msg the message
     * @param t the throwable cause
     */
    public MongoInternalException( String msg , Throwable t ){
        super(msg, t);
    }

}

