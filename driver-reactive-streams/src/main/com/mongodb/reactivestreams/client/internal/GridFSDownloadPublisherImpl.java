/*
 * Copyright 2008-present MongoDB, Inc.
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

package com.mongodb.reactivestreams.client.internal;

import com.mongodb.Block;
import com.mongodb.client.gridfs.model.GridFSFile;
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.async.client.gridfs.AsyncGridFSDownloadStream;
import com.mongodb.reactivestreams.client.gridfs.GridFSDownloadPublisher;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.nio.Buffer;
import java.nio.ByteBuffer;


public class GridFSDownloadPublisherImpl implements GridFSDownloadPublisher {
    private final AsyncGridFSDownloadStream gridFSDownloadStream;
    private int bufferSizeBytes;

    GridFSDownloadPublisherImpl(final AsyncGridFSDownloadStream gridFSDownloadStream) {
        this.gridFSDownloadStream = gridFSDownloadStream;
    }

    @Override
    public Publisher<GridFSFile> getGridFSFile() {
        return Publishers.publish(gridFSDownloadStream::getGridFSFile);
    }

    @Override
    public GridFSDownloadPublisher bufferSizeBytes(final int bufferSizeBytes) {
        this.bufferSizeBytes = bufferSizeBytes;
        return this;
    }

    @Override
    public void subscribe(final Subscriber<? super ByteBuffer> s) {
        s.onSubscribe(new GridFSDownloadSubscription(s));
    }

    class GridFSDownloadSubscription implements Subscription {
        private final Subscriber<? super ByteBuffer> outerSubscriber;

        /* protected by `this` */
        private GridFSFile gridFSFile;
        private long sizeRead = 0;
        private long requested = 0;
        private boolean unsubscribed;
        private int currentBatchSize = 0;
        private Action currentAction = Action.WAITING;
        /* protected by `this` */

        GridFSDownloadSubscription(final Subscriber<? super ByteBuffer> outerSubscriber) {
            this.outerSubscriber = outerSubscriber;
        }

        private final Subscriber<GridFSFile> gridFSFileSubscriber = new Subscriber<GridFSFile>() {
            @Override
            public void onSubscribe(final Subscription s) {
                s.request(1);
            }

            @Override
            public void onNext(final GridFSFile result) {
                synchronized (GridFSDownloadSubscription.this) {
                    gridFSFile = result;
                }
            }

            @Override
            public void onError(final Throwable t) {
                outerSubscriber.onError(t);
                terminate();
            }

            @Override
            public void onComplete() {
                synchronized (GridFSDownloadSubscription.this) {
                    currentAction = Action.WAITING;
                }
                tryProcess();
            }
        };

        class GridFSDownloadStreamSubscriber implements Subscriber<Integer> {
            private final ByteBuffer byteBuffer;

            GridFSDownloadStreamSubscriber(final ByteBuffer byteBuffer) {
                this.byteBuffer = byteBuffer;
            }

            @Override
            public void onSubscribe(final Subscription s) {
                s.request(1);
            }

            @Override
            public void onNext(final Integer integer) {
                synchronized (GridFSDownloadSubscription.this) {
                    sizeRead += integer;
                }
            }

            @Override
            public void onError(final Throwable t) {
                terminate();
                outerSubscriber.onError(t);
            }

            @Override
            public void onComplete() {
                if (byteBuffer.remaining() > 0) {
                    Publishers.publish((Block<SingleResultCallback<Integer>>) callback -> gridFSDownloadStream.read(byteBuffer, callback))
                            .subscribe(new GridFSDownloadStreamSubscriber(byteBuffer));
                } else {
                    boolean hasTerminated;
                    synchronized (GridFSDownloadSubscription.this) {
                        hasTerminated = currentAction == Action.TERMINATE || currentAction == Action.FINISHED;
                        if (!hasTerminated) {
                            currentAction = Action.WAITING;
                            if (sizeRead == gridFSFile.getLength()) {
                                currentAction = Action.COMPLETE;
                            }
                        }
                    }
                    if (!hasTerminated) {
                        ((Buffer) byteBuffer).flip();
                        outerSubscriber.onNext(byteBuffer);
                        tryProcess();
                    }
                }
            }
        }

        @Override
        public void request(final long n) {
            boolean isUnsubscribed;
            synchronized (this) {
                isUnsubscribed = unsubscribed;
                if (!isUnsubscribed && n < 1) {
                    currentAction = Action.FINISHED;
                } else {
                    requested += n;
                }
            }
            if (!isUnsubscribed && n < 1) {
                outerSubscriber.onError(new IllegalArgumentException("3.9 While the Subscription is not cancelled, "
                        + "Subscription.request(long n) MUST throw a java.lang.IllegalArgumentException if the "
                        + "argument is <= 0."));
                return;
            }
            tryProcess();
        }

        @Override
        public void cancel() {
            synchronized (this) {
                unsubscribed = true;
            }
            terminate();
        }

        private void tryProcess() {
            NextStep nextStep;
            synchronized (this) {
                switch (currentAction) {
                    case WAITING:
                        if (requested == 0) {
                            nextStep = NextStep.DO_NOTHING;
                        } else if (gridFSFile == null) {
                            nextStep = NextStep.GET_FILE;
                            currentAction = Action.IN_PROGRESS;
                        } else if (sizeRead == gridFSFile.getLength()) {
                            nextStep = NextStep.COMPLETE;
                            currentAction = Action.FINISHED;
                        } else {
                            requested--;
                            nextStep = NextStep.READ;
                            currentAction = Action.IN_PROGRESS;
                        }
                        break;
                    case COMPLETE:
                        nextStep = NextStep.COMPLETE;
                        currentAction = Action.FINISHED;
                        break;
                    case TERMINATE:
                        nextStep = NextStep.TERMINATE;
                        currentAction = Action.FINISHED;
                        break;
                    case IN_PROGRESS:
                    case FINISHED:
                    default:
                        nextStep = NextStep.DO_NOTHING;
                        break;
                }
            }

            switch (nextStep) {
                case GET_FILE:
                    getGridFSFile().subscribe(gridFSFileSubscriber);
                    break;
                case READ:
                    int chunkSize;
                    long remaining;
                    synchronized (this) {
                        chunkSize = gridFSFile.getChunkSize();
                        remaining = gridFSFile.getLength() - sizeRead;
                    }

                    int byteBufferSize = Math.max(chunkSize, bufferSizeBytes);
                    if (remaining < Integer.MAX_VALUE) {
                        byteBufferSize = Math.min(Long.valueOf(remaining).intValue(), byteBufferSize);
                    }
                    ByteBuffer byteBuffer = ByteBuffer.allocate(byteBufferSize);

                    if (currentBatchSize == 0) {
                        currentBatchSize = Math.max(byteBufferSize / chunkSize, 1);
                        gridFSDownloadStream.batchSize(currentBatchSize);
                    }
                    Publishers.publish((Block<SingleResultCallback<Integer>>) callback -> gridFSDownloadStream.read(byteBuffer, callback))
                            .subscribe(new GridFSDownloadStreamSubscriber(byteBuffer));
                    break;
                case COMPLETE:
                case TERMINATE:
                    final boolean propagateToOuter = nextStep == NextStep.COMPLETE;
                    Publishers.publish(gridFSDownloadStream::close).subscribe(new Subscriber<Void>() {
                        @Override
                        public void onSubscribe(final Subscription s) {
                            s.request(1);
                        }

                        @Override
                        public void onNext(final Void result) {
                        }

                        @Override
                        public void onError(final Throwable t) {
                            if (propagateToOuter) {
                                outerSubscriber.onError(t);
                            }
                        }

                        @Override
                        public void onComplete() {
                            if (propagateToOuter) {
                                outerSubscriber.onComplete();
                            }
                        }
                    });
                    break;
                case DO_NOTHING:
                default:
                    break;
            }
        }

        private void terminate() {
            synchronized (this) {
                currentAction = Action.TERMINATE;
            }
            tryProcess();
        }
    }

    enum Action {
        WAITING,
        IN_PROGRESS,
        TERMINATE,
        COMPLETE,
        FINISHED
    }

    enum NextStep {
        GET_FILE,
        READ,
        COMPLETE,
        TERMINATE,
        DO_NOTHING
    }
}
