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

package com.mongodb.util;

abstract class AbstractObjectSerializer implements ObjectSerializer {

    @Override
    public String serialize(final Object obj) {
        StringBuilder builder = new StringBuilder();
        serialize(obj, builder);
        return builder.toString();
    }
}
