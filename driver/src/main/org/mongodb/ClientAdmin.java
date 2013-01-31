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

package org.mongodb;

/**
 * All the commands that can be run without needing a specific database.
 */
public interface ClientAdmin {
    /**
     * @return a non-null number if the server is reachable.
     */
    //TODO: it's not clear from the documentation what the return type should be
    //http://docs.mongodb.org/manual/reference/command/ping/
    double ping();
}
