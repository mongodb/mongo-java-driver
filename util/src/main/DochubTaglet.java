/*
 * Copyright 2008-present MongoDB, Inc.
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

import com.sun.tools.doclets.Taglet;

import java.util.Map;

public class DochubTaglet extends DocTaglet {

    public static void register(final Map<String, Taglet> tagletMap) {
        DochubTaglet t = new DochubTaglet();
        tagletMap.put(t.getName(), t);
    }

    public String getName() {
        return "mongodb.driver.dochub";
    }

    @Override
    protected String getHeader() {
        return "MongoDB documentation";
    }

    @Override
    protected String getBaseDocURI() {
        return "http://dochub.mongodb.org/";
    }

}
