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

package org.mongodb.test;

import org.bson.types.ObjectId;

import java.util.Date;

public final class Worker {
    private ObjectId id;
    private String name;
    private String jobTitle;
    private Date dateStarted;
    private int numberOfJobs;

    public Worker(final String name, final String jobTitle, final Date dateStarted, final int numberOfJobs) {
        this(new ObjectId(), name, jobTitle, dateStarted, numberOfJobs);
    }

    public Worker(final ObjectId id, final String name, final String jobTitle, final Date dateStarted, final int numberOfJobs) {
        this.id = id;
        this.name = name;
        this.jobTitle = jobTitle;
        this.dateStarted = dateStarted;
        this.numberOfJobs = numberOfJobs;
    }

    public ObjectId getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public String getJobTitle() {
        return jobTitle;
    }

    public Date getDateStarted() {
        return dateStarted;
    }

    public int getNumberOfJobs() {
        return numberOfJobs;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final Worker worker = (Worker) o;

        if (numberOfJobs != worker.numberOfJobs) {
            return false;
        }
        if (!dateStarted.equals(worker.dateStarted)) {
            return false;
        }
        if (!id.equals(worker.id)) {
            return false;
        }
        if (!jobTitle.equals(worker.jobTitle)) {
            return false;
        }
        if (!name.equals(worker.name)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode() {
        int result = id.hashCode();
        result = 31 * result + name.hashCode();
        result = 31 * result + jobTitle.hashCode();
        result = 31 * result + dateStarted.hashCode();
        result = 31 * result + numberOfJobs;
        return result;
    }

    @Override
    public String toString() {
        return "Worker{"
               + "id=" + id
               + ", name='" + name + '\''
               + ", jobTitle='" + jobTitle + '\''
               + ", dateStarted=" + dateStarted
               + ", numberOfJobs=" + numberOfJobs
               + '}';
    }
}
