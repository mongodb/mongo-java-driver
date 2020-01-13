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
import com.mongodb.internal.async.SingleResultCallback;
import com.mongodb.internal.async.client.gridfs.AsyncGridFSUploadStream;
import com.mongodb.reactivestreams.client.gridfs.GridFSUploadPublisher;
import org.bson.BsonValue;
import org.bson.types.ObjectId;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import java.nio.ByteBuffer;


public class GridFSUploadPublisherImpl implements GridFSUploadPublisher<Void> {
    private final AsyncGridFSUploadStream gridFSUploadStream;
    private final Publisher<ByteBuffer> source;

    GridFSUploadPublisherImpl(final AsyncGridFSUploadStream gridFSUploadStream, final Publisher<ByteBuffer> source) {
        this.gridFSUploadStream = gridFSUploadStream;
        this.source = source;
    }

    @Override
    public ObjectId getObjectId() {
        return gridFSUploadStream.getObjectId();
    }

    @Override
    public BsonValue getId() {
        return gridFSUploadStream.getId();
    }

    @Override
    public void subscribe(final Subscriber<? super Void> s) {
        s.onSubscribe(new GridFSUploadSubscription(s));
    }

    class GridFSUploadSubscription implements Subscription {
        private final Subscriber<? super Void> outerSubscriber;

        /* protected by `this` */
        private boolean hasCompleted;
        private boolean unsubscribed;
        private Action currentAction = Action.WAITING;
        private Subscription sourceSubscription;
        /* protected by `this` */

        GridFSUploadSubscription(final Subscriber<? super Void> outerSubscriber) {
            this.outerSubscriber = outerSubscriber;
        }

        private final Subscriber<ByteBuffer> sourceSubscriber = new Subscriber<ByteBuffer>() {
            @Override
            public void onSubscribe(final Subscription s) {
                synchronized (GridFSUploadSubscription.this) {
                    sourceSubscription = s;
                    currentAction = Action.WAITING;
                }
                tryProcess();
            }

            @Override
            public void onNext(final ByteBuffer byteBuffer) {
                synchronized (GridFSUploadSubscription.this) {
                    currentAction = Action.IN_PROGRESS;
                }
                Publishers.publish((Block<SingleResultCallback<Integer>>) callback -> gridFSUploadStream.write(byteBuffer, callback))
                        .subscribe(new GridFSUploadStreamSubscriber(byteBuffer));
            }

            @Override
            public void onError(final Throwable t) {
                synchronized (GridFSUploadSubscription.this) {
                    currentAction = Action.FINISHED;
                }
                outerSubscriber.onError(t);
            }

            @Override
            public void onComplete() {
                synchronized (GridFSUploadSubscription.this) {
                    hasCompleted = true;
                    if (currentAction == Action.REQUESTING_MORE) {
                        currentAction = Action.COMPLETE;
                        tryProcess();
                    }
                }
            }


            class GridFSUploadStreamSubscriber implements Subscriber<Integer> {
                private final ByteBuffer byteBuffer;

                GridFSUploadStreamSubscriber(final ByteBuffer byteBuffer) {
                    this.byteBuffer = byteBuffer;
                }

                @Override
                public void onSubscribe(final Subscription s) {
                    s.request(1);
                }

                @Override
                public void onNext(final Integer integer) {
                }

                @Override
                public void onError(final Throwable t) {
                    terminate();
                    outerSubscriber.onError(t);
                }

                @Override
                public void onComplete() {
                    if (byteBuffer.remaining() > 0) {
                        sourceSubscriber.onNext(byteBuffer);
                    } else {
                        synchronized (GridFSUploadSubscription.this) {
                            if (hasCompleted) {
                                currentAction = Action.COMPLETE;
                            }
                            if (unsubscribed) {
                                currentAction = Action.TERMINATE;
                            }
                            if (currentAction != Action.COMPLETE && currentAction != Action.TERMINATE && currentAction != Action.FINISHED) {
                                currentAction = Action.WAITING;
                            }
                        }
                        tryProcess();
                    }
                }
            }
        };

        @Override
        public void request(final long n) {
            boolean isUnsubscribed;
            synchronized (this) {
                isUnsubscribed = unsubscribed;
                if (!isUnsubscribed && n < 1) {
                    currentAction = Action.FINISHED;
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
                        if (sourceSubscription == null) {
                            nextStep = NextStep.SUBSCRIBE;
                        } else {
                            nextStep = NextStep.REQUEST_MORE;
                        }
                        currentAction = Action.REQUESTING_MORE;
                        break;
                    case COMPLETE:
                        nextStep = NextStep.COMPLETE;
                        currentAction = Action.FINISHED;
                        break;
                    case TERMINATE:
                        nextStep = NextStep.TERMINATE;
                        currentAction = Action.FINISHED;
                        break;
                    case REQUESTING_MORE:
                    case IN_PROGRESS:
                    case FINISHED:
                    default:
                        nextStep = NextStep.DO_NOTHING;
                        break;
                }
            }

            switch (nextStep) {
                case SUBSCRIBE:
                    source.subscribe(sourceSubscriber);
                    break;
                case REQUEST_MORE:
                    synchronized (this) {
                        sourceSubscription.request(1);
                    }
                    break;
                case COMPLETE:
                    Publishers.publish(gridFSUploadStream::close).subscribe(new Subscriber<Void>() {
                        @Override
                        public void onSubscribe(final Subscription s) {
                            s.request(1);
                        }

                        @Override
                        public void onNext(final Void success) {
                            outerSubscriber.onNext(success);
                        }

                        @Override
                        public void onError(final Throwable t) {
                            outerSubscriber.onError(t);
                        }

                        @Override
                        public void onComplete() {
                            outerSubscriber.onComplete();
                        }
                    });
                    break;
                case TERMINATE:
                    Publishers.publish(gridFSUploadStream::abort).subscribe(new Subscriber<Void>() {
                        @Override
                        public void onSubscribe(final Subscription s) {
                            s.request(1);
                        }

                        @Override
                        public void onNext(final Void success) {
                        }

                        @Override
                        public void onError(final Throwable t) {
                        }

                        @Override
                        public void onComplete() {
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

    GridFSUploadPublisher<ObjectId> withObjectId() {
        final GridFSUploadPublisherImpl wrapped = this;
        return new GridFSUploadPublisher<ObjectId>() {

            @Override
            public ObjectId getObjectId() {
                return wrapped.getObjectId();
            }

            @Override
            public BsonValue getId() {
                return wrapped.getId();
            }

            @Override
            public void subscribe(final Subscriber<? super ObjectId> objectIdSub) {
                wrapped.subscribe(new Subscriber<Void>() {
                    @Override
                    public void onSubscribe(final Subscription s) {
                        objectIdSub.onSubscribe(s);
                    }

                    @Override
                    public void onNext(final Void success) {
                    }

                    @Override
                    public void onError(final Throwable t) {
                        objectIdSub.onError(t);
                    }

                    @Override
                    public void onComplete() {
                        objectIdSub.onNext(getObjectId());
                        objectIdSub.onComplete();
                    }
                });
            }
        };
    }

    enum Action {
        WAITING,
        REQUESTING_MORE,
        IN_PROGRESS,
        TERMINATE,
        COMPLETE,
        FINISHED
    }

    enum NextStep {
        SUBSCRIBE,
        REQUEST_MORE,
        COMPLETE,
        TERMINATE,
        DO_NOTHING
    }
}
