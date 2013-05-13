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

package org.mongodb.impl;

import org.mongodb.MongoException;
import org.mongodb.ServerAddress;
import org.mongodb.annotations.ThreadSafe;
import org.mongodb.command.IsMasterCommandResult;
import org.mongodb.rs.ReplicaSet;
import org.mongodb.rs.ReplicaSetMember;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.mongodb.assertions.Assertions.isTrue;
import static org.mongodb.impl.MonitorDefaults.CLIENT_OPTIONS_DEFAULTS;
import static org.mongodb.impl.MonitorDefaults.LATENCY_SMOOTH_FACTOR;
import static org.mongodb.impl.MonitorDefaults.SLAVE_ACCEPTABLE_LATENCY_MS;
import static org.mongodb.impl.MonitorDefaults.UPDATER_INTERVAL_MS;

// TODO:
// 1. Handle server address mis-matches
// 2. Handle un-discovery
// 3. Reconsider class names
// 5. Be able to response before hearing from all replica set members.
// 6. Handle socket exceptions by refreshing state

/**
 * Monitors the state of a replica set.  Uses a configurable thread pool to periodically check the state of of member by calling the
 * ismaster command.
 */
@ThreadSafe
class ReplicaSetMonitor implements MongoServerStateListener {

    private static final Logger LOGGER = Logger.getLogger("org.mongodb.ReplicaSetMonitor");

    // will get changed to use replica set name once it's found
    private volatile Logger logger = LOGGER;

    private MongoServerStateNotifierFactory serverStateNotifierFactory;
    private ScheduledExecutorService scheduledExecutorService;
    private final Holder<ReplicaSet> holder = new Holder<ReplicaSet>(CLIENT_OPTIONS_DEFAULTS.getConnectTimeout(), TimeUnit.MILLISECONDS);
    private List<ServerAddress> serverList;
    private final Map<ServerAddress, ReplicaSetMember> mostRecentStateMap = new HashMap<ServerAddress, ReplicaSetMember>();
    private final Map<ServerAddress, ScheduledFuture<?>> futureMap = new HashMap<ServerAddress, ScheduledFuture<?>>();
    private final Map<ServerAddress, MongoServerStateNotifier> notifierMap = new HashMap<ServerAddress, MongoServerStateNotifier>();
    private final Map<ServerAddress, Boolean> activeMemberNotifications = new HashMap<ServerAddress, Boolean>();
    private final Random random = new Random();

    ReplicaSetMonitor(final List<ServerAddress> seedList) {
        serverList = new ArrayList<ServerAddress>(seedList);
    }

    public synchronized void start(final MongoServerStateNotifierFactory newServerStateNotifierFactory,
                                   final ScheduledExecutorService newScheduledExecutorService) {
        isTrue("open", !isShutdown());
        isTrue("not already started", scheduledExecutorService == null);

        serverStateNotifierFactory = newServerStateNotifierFactory;
        scheduledExecutorService = newScheduledExecutorService;
        addNewServerAddresses(serverList);
    }

    ReplicaSet getCurrentState() {
        isTrue("open", !isShutdown());
        return holder.get();
    }

    ReplicaSet getCurrentState(final long timeout, final TimeUnit timeUnit) {
        isTrue("open", !isShutdown());
        return holder.get(timeout, timeUnit);
    }

    @Override
    public synchronized void notify(final ServerAddress serverAddress, final IsMasterCommandResult isMasterCommandResult) {
        if (isShutdown()) {
            return;
        }

        if (!futureMap.containsKey(serverAddress)) {
            return;
        }

        markAsNotified(serverAddress);

        addNewHosts(isMasterCommandResult.getHosts(), true);
        addNewHosts(isMasterCommandResult.getPassives(), false);
        if (isMasterCommandResult.isPrimary()) {
            removeExtras(isMasterCommandResult);
        }

        mostRecentStateMap.put(serverAddress, new ReplicaSetMember(serverAddress, isMasterCommandResult, LATENCY_SMOOTH_FACTOR,
                mostRecentStateMap.get(serverAddress)));

        addToHolder();

        setLoggerName(isMasterCommandResult);
    }

    @Override
    public synchronized void notify(final ServerAddress serverAddress, final MongoException e) {
        if (isShutdown()) {
            return;
        }

        markAsNotified(serverAddress);

        mostRecentStateMap.remove(serverAddress);

        addToHolder();

        logger.log(Level.FINE, "Exception retrieving state for " + serverAddress, e);
    }

    private void markAsNotified(final ServerAddress serverAddress) {
        if (activeMemberNotifications.containsKey(serverAddress)) {
            activeMemberNotifications.put(serverAddress, true);
        }
    }

    /**
     * Stop monitoring the replica set.  Once shutdown, the instance can no longer be used, and clients can no longer get the current
     * state of the replica set.
     */
    public void shutdownNow() { // TODO: synchronized
        if (!isShutdown()) {
            holder.close();
            if (scheduledExecutorService != null) {
                scheduledExecutorService.shutdownNow();
                for (MongoServerStateNotifier notifier : notifierMap.values()) {
                    notifier.close();
                }
            }
        }
    }

    /**
     * Returns true if this instance has bee shutdown.
     *
     * @return true if shut down, otherwise false
     */
    public boolean isShutdown() {
        return holder.isClosed();
    }

    private void setLoggerName(final IsMasterCommandResult isMasterCommandResult) {
        if (isMasterCommandResult.getSetName() != null) {
            logger = Logger.getLogger(logger.getName() + "." + isMasterCommandResult.getSetName());
        }
    }

    private void addToHolder() {
        if (holder.peek() != null || allActiveMembersHaveNotified()) {
            setHolder();
        }
    }

    private boolean allActiveMembersHaveNotified() {
        for (boolean notified : activeMemberNotifications.values()) {
            if (!notified) {
                return false;
            }
        }
        return true;
    }

    private void setHolder() {
        holder.set(new ReplicaSet(new ArrayList<ReplicaSetMember>(mostRecentStateMap.values()), random, SLAVE_ACCEPTABLE_LATENCY_MS));
    }

    private void addNewServerAddresses(final List<ServerAddress> hosts) {
        for (ServerAddress cur : hosts) {
            activeMemberNotifications.put(cur, false);
            addToSchedule(cur);
        }
    }

    private void addNewHosts(final List<String> hosts, final boolean active) {
        for (String cur : hosts) {
            ServerAddress serverAddress = getServerAddress(cur);
            if (serverAddress != null) {
                if (active && !activeMemberNotifications.containsKey(serverAddress)) {
                    activeMemberNotifications.put(serverAddress, false);
                }
                addToSchedule(serverAddress);
            }
        }
    }

    private void addToSchedule(final ServerAddress serverAddress) {
        if (serverAddress != null && !futureMap.containsKey(serverAddress)) {  // TODO
            final MongoServerStateNotifier serverStateNotifier = serverStateNotifierFactory.create(serverAddress);
            notifierMap.put(serverAddress, serverStateNotifier);
            futureMap.put(serverAddress, scheduledExecutorService.scheduleAtFixedRate(serverStateNotifier,
                    0, UPDATER_INTERVAL_MS, TimeUnit.MILLISECONDS));
        }
    }

    private void removeExtras(final IsMasterCommandResult isMasterCommandResult) {
        Set<ServerAddress> allServerAddresses = getAllServerAddresses(isMasterCommandResult);
        for (Iterator<ServerAddress> iter = futureMap.keySet().iterator(); iter.hasNext();) {
            ServerAddress host = iter.next();
            if (!allServerAddresses.contains(host)) {
                ScheduledFuture<?> future = futureMap.get(host);
                future.cancel(true);
                iter.remove();
                MongoServerStateNotifier notifier = notifierMap.remove(host);
                notifier.close();
                activeMemberNotifications.remove(host);
                mostRecentStateMap.remove(host);
            }
        }
    }

    // TODO: move these next two methods
    private Set<ServerAddress> getAllServerAddresses(final IsMasterCommandResult masterCommandResult) {
        Set<ServerAddress> retVal = new HashSet<ServerAddress>();
        addHostsToSet(masterCommandResult.getHosts(), retVal);
        addHostsToSet(masterCommandResult.getPassives(), retVal);
        return retVal;
    }

    private void addHostsToSet(final List<String> hosts, final Set<ServerAddress> retVal) {
        for (String host : hosts) {
            ServerAddress serverAddress = getServerAddress(host);
            if (serverAddress != null) {
                retVal.add(serverAddress);
            }
        }
    }

    private ServerAddress getServerAddress(final String serverAddressString) {
        try {
            return new ServerAddress(serverAddressString);
        } catch (UnknownHostException e) {
            return null;
        }
    }
}

