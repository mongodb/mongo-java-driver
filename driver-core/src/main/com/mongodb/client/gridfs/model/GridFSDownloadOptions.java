/*
 * Copyright 2016 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.mongodb.client.gridfs.model;

/**
 * The GridFS download by name options
 *
 * <p>Controls the selection of the revision to download</p>
 *
 * @since 3.3
 */
public final class GridFSDownloadOptions {
    private int revision;

    /**
     * Download the most recent version of the file.
     *
     * <p>Defaults to the most recent revision.</p>
     */
    public GridFSDownloadOptions() {
        revision = -1;
    }

    /**
     * Set the revision of the file to retrieve.
     *
     * <p>Revision numbers are defined as follows:</p>
     * <ul>
     *  <li><strong>0</strong> = the original stored file</li>
     *  <li><strong>1</strong> = the first revision</li>
     *  <li><strong>2</strong> = the second revision</li>
     *  <li>etc..</li>
     *  <li><strong>-2</strong> = the second most recent revision</li>
     *  <li><strong>-1</strong> = the most recent revision</li>
     * </ul>
     *
     *
     * @param revision the file revision to download
     * @return this
     */
    public GridFSDownloadOptions revision(final int revision) {
        this.revision = revision;
        return this;
    }

    /**
     * Gets the revision to download identifier
     *
     * @return the revision to download identifier
     */
    public int getRevision() {
        return revision;
    }
}
