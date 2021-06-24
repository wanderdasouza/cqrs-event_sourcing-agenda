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

package com.mongodb.internal.connection;

import com.mongodb.MongoException;
import com.mongodb.MongoInternalException;
import com.mongodb.MongoInterruptedException;
import com.mongodb.MongoSocketException;
import com.mongodb.MongoSocketReadTimeoutException;
import com.mongodb.MongoTimeoutException;
import com.mongodb.MongoWaitQueueFullException;
import com.mongodb.async.SingleResultCallback;
import com.mongodb.connection.ConnectionDescription;
import com.mongodb.connection.ConnectionId;
import com.mongodb.connection.ConnectionPoolSettings;
import com.mongodb.connection.ServerId;
import com.mongodb.diagnostics.logging.Logger;
import com.mongodb.diagnostics.logging.Loggers;
import com.mongodb.event.ConnectionAddedEvent;
import com.mongodb.event.ConnectionCheckedInEvent;
import com.mongodb.event.ConnectionCheckedOutEvent;
import com.mongodb.event.ConnectionPoolClosedEvent;
import com.mongodb.event.ConnectionPoolListener;
import com.mongodb.event.ConnectionPoolOpenedEvent;
import com.mongodb.event.ConnectionPoolWaitQueueEnteredEvent;
import com.mongodb.event.ConnectionPoolWaitQueueExitedEvent;
import com.mongodb.event.ConnectionRemovedEvent;
import com.mongodb.internal.connection.ConcurrentPool.Prune;
import com.mongodb.internal.thread.DaemonThreadFactory;
import com.mongodb.session.SessionContext;
import org.bson.ByteBuf;
import org.bson.codecs.Decoder;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static com.mongodb.assertions.Assertions.isTrue;
import static com.mongodb.assertions.Assertions.notNull;
import static com.mongodb.internal.async.ErrorHandlingResultCallback.errorHandlingCallback;
import static com.mongodb.internal.event.EventListenerHelper.getConnectionPoolListener;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

class DefaultConnectionPool implements ConnectionPool {
    private static final Logger LOGGER = Loggers.getLogger("connection");

    private final ConcurrentPool<UsageTrackingInternalConnection> pool;
    private final ConnectionPoolSettings settings;
    private final AtomicInteger waitQueueSize = new AtomicInteger(0);
    private final AtomicInteger generation = new AtomicInteger(0);
    private final AtomicInteger lastPrunedGeneration = new AtomicInteger(0);
    private final ScheduledExecutorService sizeMaintenanceTimer;
    private ExecutorService asyncGetter;
    private final Runnable maintenanceTask;
    private final ConnectionPoolListener connectionPoolListener;
    private final ServerId serverId;
    private volatile boolean closed;

    DefaultConnectionPool(final ServerId serverId, final InternalConnectionFactory internalConnectionFactory,
                          final ConnectionPoolSettings settings) {
        this.serverId = notNull("serverId", serverId);
        this.settings = notNull("settings", settings);
        UsageTrackingInternalConnectionItemFactory connectionItemFactory =
                new UsageTrackingInternalConnectionItemFactory(internalConnectionFactory);
        pool = new ConcurrentPool<UsageTrackingInternalConnection>(settings.getMaxSize(), connectionItemFactory);
        this.connectionPoolListener = getConnectionPoolListener(settings);
        maintenanceTask = createMaintenanceTask();
        sizeMaintenanceTimer = createMaintenanceTimer();
        connectionPoolListener.connectionPoolOpened(new ConnectionPoolOpenedEvent(serverId, settings));
    }

    @Override
    public void start() {
        if (sizeMaintenanceTimer != null) {
            sizeMaintenanceTimer.scheduleAtFixedRate(maintenanceTask, settings.getMaintenanceInitialDelay(MILLISECONDS),
                    settings.getMaintenanceFrequency(MILLISECONDS), MILLISECONDS);
        }
    }

    @Override
    public InternalConnection get() {
        return get(settings.getMaxWaitTime(MILLISECONDS), MILLISECONDS);
    }

    @Override
    public InternalConnection get(final long timeout, final TimeUnit timeUnit) {
        try {
            if (waitQueueSize.incrementAndGet() > settings.getMaxWaitQueueSize()) {
                throw createWaitQueueFullException();
            }
            try {
                connectionPoolListener.waitQueueEntered(new ConnectionPoolWaitQueueEnteredEvent(serverId));
                PooledConnection pooledConnection = getPooledConnection(timeout, timeUnit);
                if (!pooledConnection.opened()) {
                    try {
                        pooledConnection.open();
                    } catch (Throwable t) {
                        pool.release(pooledConnection.wrapped, true);
                        if (t instanceof MongoException) {
                            throw (MongoException) t;
                        } else {
                            throw new MongoInternalException(t.toString(), t);
                        }
                    }
                }

                return pooledConnection;
            } finally {
                connectionPoolListener.waitQueueExited(new ConnectionPoolWaitQueueExitedEvent(serverId));
            }
        } finally {
            waitQueueSize.decrementAndGet();
        }
    }

    @Override
    public void getAsync(final SingleResultCallback<InternalConnection> callback) {
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Asynchronously getting a connection from the pool for server %s", serverId));
        }

        final SingleResultCallback<InternalConnection> errHandlingCallback = errorHandlingCallback(callback, LOGGER);
        PooledConnection connection = null;

        try {
            connection = getPooledConnection(0, MILLISECONDS);
        } catch (MongoTimeoutException e) {
            // fall through
        } catch (Throwable t) {
            callback.onResult(null, t);
            return;
        }

        if (connection != null) {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace(format("Asynchronously opening pooled connection %s to server %s",
                                           connection.getDescription().getConnectionId(), serverId));
            }
            openAsync(connection, errHandlingCallback);
        } else if (waitQueueSize.incrementAndGet() > settings.getMaxWaitQueueSize()) {
            waitQueueSize.decrementAndGet();
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace(format("Asynchronously failing to get a pooled connection to %s because the wait queue is full",
                                           serverId));
            }
            callback.onResult(null, createWaitQueueFullException());
        } else {
            final long startTimeMillis = System.currentTimeMillis();
            connectionPoolListener.waitQueueEntered(new ConnectionPoolWaitQueueEnteredEvent(serverId));
            getAsyncGetter().submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        if (getRemainingWaitTime() <= 0) {
                            errHandlingCallback.onResult(null, createTimeoutException());
                        } else {
                            PooledConnection connection = getPooledConnection(getRemainingWaitTime(), MILLISECONDS);
                            openAsync(connection, errHandlingCallback);
                        }
                    } catch (Throwable t) {
                        errHandlingCallback.onResult(null, t);
                    } finally {
                        waitQueueSize.decrementAndGet();
                        connectionPoolListener.waitQueueExited(new ConnectionPoolWaitQueueExitedEvent(serverId));
                    }
                }

                private long getRemainingWaitTime() {
                    return startTimeMillis + settings.getMaxWaitTime(MILLISECONDS) - System.currentTimeMillis();
                }
            });
        }
    }

    private void openAsync(final PooledConnection pooledConnection,
                           final SingleResultCallback<InternalConnection> callback) {
        if (pooledConnection.opened()) {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace(format("Pooled connection %s to server %s is already open",
                                           pooledConnection.getDescription().getConnectionId(), serverId));
            }
            callback.onResult(pooledConnection, null);
        } else {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace(format("Pooled connection %s to server %s is not yet open",
                                           pooledConnection.getDescription().getConnectionId(), serverId));
            }
            pooledConnection.openAsync(new SingleResultCallback<Void>() {
                @Override
                public void onResult(final Void result, final Throwable t) {
                    if (t != null) {
                        if (LOGGER.isTraceEnabled()) {
                            LOGGER.trace(format("Pooled connection %s to server %s failed to open",
                                                       pooledConnection.getDescription().getConnectionId(), serverId));
                        }
                        callback.onResult(null, t);
                        pool.release(pooledConnection.wrapped, true);
                    } else {
                        if (LOGGER.isTraceEnabled()) {
                            LOGGER.trace(format("Pooled connection %s to server %s is now open",
                                                       pooledConnection.getDescription().getConnectionId(), serverId));
                        }
                        callback.onResult(pooledConnection, null);
                    }
                }
            });
        }
    }

    private synchronized ExecutorService getAsyncGetter() {
        if (asyncGetter == null) {
            asyncGetter = Executors.newSingleThreadExecutor(new DaemonThreadFactory("AsyncGetter"));
        }
        return asyncGetter;
    }

    private synchronized void shutdownAsyncGetter() {
        if (asyncGetter != null) {
            asyncGetter.shutdownNow();
        }
    }

    @Override
    public void invalidate() {
        LOGGER.debug("Invalidating the connection pool");
        generation.incrementAndGet();
    }

    @Override
    public void close() {
        if (!closed) {
            pool.close();
            if (sizeMaintenanceTimer != null) {
                sizeMaintenanceTimer.shutdownNow();
            }
            shutdownAsyncGetter();
            closed = true;
            connectionPoolListener.connectionPoolClosed(new ConnectionPoolClosedEvent(serverId));
        }
    }

    /**
     * Synchronously prune idle connections and ensure the minimum pool size.
     */
    public void doMaintenance() {
        if (maintenanceTask != null) {
            maintenanceTask.run();
        }
    }

    private PooledConnection getPooledConnection(final long timeout, final TimeUnit timeUnit) {
        UsageTrackingInternalConnection internalConnection = pool.get(timeout, timeUnit);
        while (shouldPrune(internalConnection)) {
            pool.release(internalConnection, true);
            internalConnection = pool.get(timeout, timeUnit);
        }
        connectionPoolListener.connectionCheckedOut(new ConnectionCheckedOutEvent(internalConnection.getDescription().getConnectionId()));
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace(format("Checked out connection [%s] to server %s", getId(internalConnection), serverId.getAddress()));
        }
        return new PooledConnection(internalConnection);
    }

    private MongoTimeoutException createTimeoutException() {
        return new MongoTimeoutException(format("Timed out after %d ms while waiting for a connection to server %s.",
                                                settings.getMaxWaitTime(MILLISECONDS), serverId.getAddress()));
    }

    private MongoWaitQueueFullException createWaitQueueFullException() {
        return new MongoWaitQueueFullException(format("Too many operations are already waiting for a connection. "
                                                      + "Max number of operations (maxWaitQueueSize) of %d has been exceeded.",
                                                      settings.getMaxWaitQueueSize()));
    }

    ConcurrentPool<UsageTrackingInternalConnection> getPool() {
        return pool;
    }

    private Runnable createMaintenanceTask() {
        Runnable newMaintenanceTask = null;
        if (shouldPrune() || shouldEnsureMinSize()) {
            newMaintenanceTask = new Runnable() {
                @Override
                public synchronized void run() {
                    try {
                        int curGeneration = generation.get();
                        if (shouldPrune() || curGeneration > lastPrunedGeneration.get()) {
                            if (LOGGER.isDebugEnabled()) {
                                LOGGER.debug(format("Pruning pooled connections to %s", serverId.getAddress()));
                            }
                            pool.prune();
                        }
                        lastPrunedGeneration.set(curGeneration);
                        if (shouldEnsureMinSize()) {
                            if (LOGGER.isDebugEnabled()) {
                                LOGGER.debug(format("Ensuring minimum pooled connections to %s", serverId.getAddress()));
                            }
                            pool.ensureMinSize(settings.getMinSize(), true);
                        }
                    } catch (MongoInterruptedException e) {
                        // don't log interruptions due to the shutdownNow call on the ExecutorService
                    } catch (Exception e) {
                        LOGGER.warn("Exception thrown during connection pool background maintenance task", e);
                    }
                }
            };
        }
        return newMaintenanceTask;
    }

    private ScheduledExecutorService createMaintenanceTimer() {
        if (maintenanceTask == null) {
            return null;
        } else {
            return Executors.newSingleThreadScheduledExecutor(new DaemonThreadFactory("MaintenanceTimer"));
        }
    }

    private boolean shouldEnsureMinSize() {
        return settings.getMinSize() > 0;
    }

    private boolean shouldPrune() {
        return settings.getMaxConnectionIdleTime(MILLISECONDS) > 0 || settings.getMaxConnectionLifeTime(MILLISECONDS) > 0;
    }

    private boolean shouldPrune(final UsageTrackingInternalConnection connection) {
        return fromPreviousGeneration(connection) || pastMaxLifeTime(connection) || pastMaxIdleTime(connection);
    }

    private boolean pastMaxIdleTime(final UsageTrackingInternalConnection connection) {
        return expired(connection.getLastUsedAt(), System.currentTimeMillis(), settings.getMaxConnectionIdleTime(MILLISECONDS));
    }

    private boolean pastMaxLifeTime(final UsageTrackingInternalConnection connection) {
        return expired(connection.getOpenedAt(), System.currentTimeMillis(), settings.getMaxConnectionLifeTime(MILLISECONDS));
    }

    private boolean fromPreviousGeneration(final UsageTrackingInternalConnection connection) {
        return generation.get() > connection.getGeneration();
    }

    private boolean expired(final long startTime, final long curTime, final long maxTime) {
        return maxTime != 0 && curTime - startTime > maxTime;
    }

    /**
     * If there was a socket exception that wasn't some form of interrupted read, increment the generation count so that any connections
     * created prior will be discarded.
     *
     * @param connection the connection that generated the exception
     * @param t          the exception
     */
    private void incrementGenerationOnSocketException(final InternalConnection connection, final Throwable t) {
        if (t instanceof MongoSocketException && !(t instanceof MongoSocketReadTimeoutException)) {
            if (LOGGER.isWarnEnabled()) {
                LOGGER.warn(format("Got socket exception on connection [%s] to %s. All connections to %s will be closed.",
                                   getId(connection), serverId.getAddress(), serverId.getAddress()));
            }
            invalidate();
        }
    }

    private ConnectionId getId(final InternalConnection internalConnection) {
        return internalConnection.getDescription().getConnectionId();
    }

    private class PooledConnection implements InternalConnection {
        private final UsageTrackingInternalConnection wrapped;
        private final AtomicBoolean isClosed = new AtomicBoolean();

        PooledConnection(final UsageTrackingInternalConnection wrapped) {
            this.wrapped = notNull("wrapped", wrapped);
        }

        @Override
        public void open() {
            isTrue("open", !isClosed.get());
            wrapped.open();
        }

        @Override
        public void openAsync(final SingleResultCallback<Void> callback) {
            isTrue("open", !isClosed.get());
            wrapped.openAsync(callback);
        }

        @Override
        public void close() {
            // All but the first call is a no-op
            if (!isClosed.getAndSet(true)) {
                connectionPoolListener.connectionCheckedIn(new ConnectionCheckedInEvent(getId(wrapped)));
                if (LOGGER.isTraceEnabled()) {
                    LOGGER.trace(format("Checked in connection [%s] to server %s", getId(wrapped), serverId.getAddress()));
                }
                pool.release(wrapped, wrapped.isClosed() || shouldPrune(wrapped));
            }
        }

        @Override
        public boolean opened() {
            isTrue("open", !isClosed.get());
            return wrapped.opened();
        }

        @Override
        public boolean isClosed() {
            return isClosed.get() || wrapped.isClosed();
        }

        @Override
        public ByteBuf getBuffer(final int capacity) {
            return wrapped.getBuffer(capacity);
        }

        @Override
        public void sendMessage(final List<ByteBuf> byteBuffers, final int lastRequestId) {
            isTrue("open", !isClosed.get());
            try {
                wrapped.sendMessage(byteBuffers, lastRequestId);
            } catch (MongoException e) {
                incrementGenerationOnSocketException(this, e);
                throw e;
            }
        }

        @Override
        public <T> T sendAndReceive(final CommandMessage message, final Decoder<T> decoder, final SessionContext sessionContext) {
            isTrue("open", !isClosed.get());
            try {
                return wrapped.sendAndReceive(message, decoder, sessionContext);
            } catch (MongoException e) {
                incrementGenerationOnSocketException(this, e);
                throw e;
            }
        }

        @Override
        public <T> void sendAndReceiveAsync(final CommandMessage message, final Decoder<T> decoder,
                                            final SessionContext sessionContext, final SingleResultCallback<T> callback) {
            isTrue("open", !isClosed.get());
            wrapped.sendAndReceiveAsync(message, decoder, sessionContext, new SingleResultCallback<T>() {
                @Override
                public void onResult(final T result, final Throwable t) {
                    if (t != null) {
                        incrementGenerationOnSocketException(PooledConnection.this, t);
                    }
                    callback.onResult(result, t);
                }
            });
        }

        @Override
        public ResponseBuffers receiveMessage(final int responseTo) {
            isTrue("open", !isClosed.get());
            try {
                return wrapped.receiveMessage(responseTo);
            } catch (MongoException e) {
                incrementGenerationOnSocketException(this, e);
                throw e;
            }
        }

        @Override
        public void sendMessageAsync(final List<ByteBuf> byteBuffers, final int lastRequestId, final SingleResultCallback<Void> callback) {
            isTrue("open", !isClosed.get());
            wrapped.sendMessageAsync(byteBuffers, lastRequestId, new SingleResultCallback<Void>() {
                @Override
                public void onResult(final Void result, final Throwable t) {
                    if (t != null) {
                        incrementGenerationOnSocketException(PooledConnection.this, t);
                    }
                    callback.onResult(null, t);
                }
            });
        }

        @Override
        public void receiveMessageAsync(final int responseTo, final SingleResultCallback<ResponseBuffers> callback) {
            isTrue("open", !isClosed.get());
            wrapped.receiveMessageAsync(responseTo, new SingleResultCallback<ResponseBuffers>() {
                @Override
                public void onResult(final ResponseBuffers result, final Throwable t) {
                    if (t != null) {
                        incrementGenerationOnSocketException(PooledConnection.this, t);
                    }
                    callback.onResult(result, t);
                }
            });
        }

        @Override
        public ConnectionDescription getDescription() {
            isTrue("open", !isClosed.get());
            return wrapped.getDescription();
        }
    }

    private class UsageTrackingInternalConnectionItemFactory implements ConcurrentPool.ItemFactory<UsageTrackingInternalConnection> {
        private final InternalConnectionFactory internalConnectionFactory;

        UsageTrackingInternalConnectionItemFactory(final InternalConnectionFactory internalConnectionFactory) {
            this.internalConnectionFactory = internalConnectionFactory;
        }

        @Override
        public UsageTrackingInternalConnection create(final boolean initialize) {
            UsageTrackingInternalConnection internalConnection =
            new UsageTrackingInternalConnection(internalConnectionFactory.create(serverId), generation.get());
            if (initialize) {
                internalConnection.open();
            }
            connectionPoolListener.connectionAdded(new ConnectionAddedEvent(getId(internalConnection)));
            return internalConnection;
        }

        @Override
        public void close(final UsageTrackingInternalConnection connection) {
            connectionPoolListener.connectionRemoved(new ConnectionRemovedEvent(getId(connection), getReasonForClosing(connection)));
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info(format("Closed connection [%s] to %s because %s.", getId(connection), serverId.getAddress(),
                                  getReasonStringForClosing(connection)));
            }
            connection.close();
        }

        private String getReasonStringForClosing(final UsageTrackingInternalConnection connection) {
            String reason;
            if (connection.isClosed()) {
                reason = "there was a socket exception raised by this connection";
            } else if (fromPreviousGeneration(connection)) {
                reason = "there was a socket exception raised on another connection from this pool";
            } else if (pastMaxLifeTime(connection)) {
                reason = "it is past its maximum allowed life time";
            } else if (pastMaxIdleTime(connection)) {
                reason = "it is past its maximum allowed idle time";
            } else {
                reason = "the pool has been closed";
            }
            return reason;
        }

        private ConnectionRemovedEvent.Reason getReasonForClosing(final UsageTrackingInternalConnection connection) {
            ConnectionRemovedEvent.Reason reason;
            if (connection.isClosed()) {
                reason = ConnectionRemovedEvent.Reason.ERROR;
            } else if (fromPreviousGeneration(connection)) {
                reason = ConnectionRemovedEvent.Reason.STALE;
            } else if (pastMaxLifeTime(connection)) {
                reason = ConnectionRemovedEvent.Reason.MAX_LIFE_TIME_EXCEEDED;
            } else if (pastMaxIdleTime(connection)) {
                reason = ConnectionRemovedEvent.Reason.MAX_IDLE_TIME_EXCEEDED;
            } else {
                reason = ConnectionRemovedEvent.Reason.POOL_CLOSED;
            }
            return reason;
        }

        @Override
        public Prune shouldPrune(final UsageTrackingInternalConnection usageTrackingConnection) {
            return DefaultConnectionPool.this.shouldPrune(usageTrackingConnection) ? Prune.YES : Prune.NO;
        }
    }
}
