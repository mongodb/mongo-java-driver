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
package com.mongodb.internal.selector;

import com.mongodb.annotations.Immutable;
import com.mongodb.connection.ClusterDescription;
import com.mongodb.connection.ServerDescription;
import com.mongodb.selector.ServerSelector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * {@linkplain #select(ClusterDescription) Selects} at most two {@link ServerDescription}s at random. This selector uses the
 * <a href="https://en.wikipedia.org/wiki/Fisher%E2%80%93Yates_shuffle">Fisher–Yates, a.k.a. Durstenfeld, shuffle algorithm</a>.
 *
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
@Immutable
public final class AtMostTwoRandomServerSelector implements ServerSelector {
    private static final int TWO = 2;
    private static final AtMostTwoRandomServerSelector INSTANCE = new AtMostTwoRandomServerSelector();

    private AtMostTwoRandomServerSelector() {
    }

    public static AtMostTwoRandomServerSelector instance() {
        return INSTANCE;
    }

    @Override
    public List<ServerDescription> select(final ClusterDescription clusterDescription) {
        List<ServerDescription> serverDescriptions = new ArrayList<>(clusterDescription.getServerDescriptions());
        List<ServerDescription> result = new ArrayList<>();
        ThreadLocalRandom random = ThreadLocalRandom.current();
        for (int i = serverDescriptions.size() - 1; i >= 0; i--) {
            Collections.swap(serverDescriptions, i, random.nextInt(i + 1));
            result.add(serverDescriptions.get(i));
            if (result.size() == TWO) {
                break;
            }
        }
        return result;
    }
}
