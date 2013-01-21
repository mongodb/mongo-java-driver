/*
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
 */

// Code.java

package org.bson.types;

import java.io.Serializable;

/**
 * for using the Code type
 */
public class Code implements Serializable {

    private final String code;

    private static final long serialVersionUID = 475535263314046697L;

    public Code(final String code) {
        this.code = code;
    }

    public String getCode() {
        return code;
    }

    public boolean equals(final Object o) {
        if (!(o instanceof Code)) {
            return false;
        }

        final Code c = (Code) o;
        return code.equals(c.code);
    }

    public int hashCode() {
        return code.hashCode();
    }

    @Override
    public String toString() {
        return getCode();
    }

}

