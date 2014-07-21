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

package com.mongodb.client;

import java.util.List;

/**
 * All the commands that can be run without needing a specific database.
 *
 * @since 3.0
 */
public interface ClientAdministration {
    /**
     * @return a non-null number if the server is reachable.
     * @mongodb.driver.manual reference/commands/ping Ping
     */
    double ping();

    /**
     * @return a List of the names of all the databases on the server
     * @mongodb.driver.manual reference/commands/listDatabases List Databases
     */
    List<String> getDatabaseNames();
}
