// ThreadPool.java

/**
 *      Copyright (C) 2008 10gen Inc.
 *  
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 * 
 *      http://www.apache.org/licenses/LICENSE-2.0
 * 
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package com.mongodb.util;

import java.util.List;
import java.util.Vector;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A thread pool implementation.
 *
 * @deprecated This class is no longer in use and should not have been public.  It may be removed in a future release.
 */
@Deprecated
public abstract class ThreadPool<T> {

    /** Initializes a new thread pool with a given name and number of threads.
     * @param name identifying name
     * @param numThreads the number of threads allowed in the pool
     */
    public ThreadPool( String name , int numThreads ){
        this( name , numThreads , Integer.MAX_VALUE );
    }

    /** Initializes a new thread pool with a given name, number of threads, and queue size.
     * @param name identifying name
     * @param numThreads the number of threads allowed in the pool
     * @param maxQueueSize the size of the pool entry queue
     */
    public ThreadPool( String name , int numThreads , int maxQueueSize ){
        _name = name;
        _maxThreads = numThreads;
        _queue = new LinkedBlockingQueue<T>( maxQueueSize );
        _myThreadGroup = new MyThreadGroup();
        _threads.add( new MyThread() );
    }

    /** Handles a given object.
     * @param t the object to handle
     * @throws Exception
     */
    public abstract void handle( T t )
        throws Exception ;

    /** Handles a given object and exception.
     * @param t the object to handle
     * @param e  the exception to handle
     */
    public abstract void handleError( T t , Exception e );

    /** Returns the size of the pool's queue.
     * @return pool size
     */
    public int queueSize(){
        return _queue.size();
    }

    /** Adds a new object to the pool, if possible.
     * @param t the object to be added
     * @return if the object was successfully added
     */
    public boolean offer( T t ){
        if ( ( _queue.size() > 0 || _inProgress.get() == _threads.size() ) && 
             _threads.size() < _maxThreads )
            _threads.add( new MyThread() );
        return _queue.offer( t );
    }

    public int inProgress(){
	return _inProgress.get();
    }

    public int numThreads(){
        return _threads.size();
    }

    class MyThreadGroup extends ThreadGroup {
        MyThreadGroup(){
            super( "ThreadPool.MyThreadGroup:" + _name );
        }

        public void uncaughtException( Thread t, Throwable e ){
            for ( int i=0; i<_threads.size(); i++ ){
                if ( _threads.get( i ) == t ){
                    _threads.remove( i );
                    break;
                }
            }
        }
    }

    class MyThread extends Thread {
        MyThread(){
            super( _myThreadGroup , "ThreadPool.MyThread:" + _name + ":" + _threads.size() );
            setDaemon( true );
            start();
        }

        public void run(){
            while ( true ){
                T t = null;

                try {
                    t = _queue.take();
                }
                catch ( InterruptedException ie ){
                }

                if ( t == null )
                    continue;

                try {
                    _inProgress.incrementAndGet();
                    handle( t );
                }
                catch ( Exception e ){
                    handleError( t , e );
                }
                finally {
                    _inProgress.decrementAndGet();
                }
            }
        }
    }

    final String _name;
    final int _maxThreads;

    private final AtomicInteger _inProgress = new AtomicInteger(0);
    private final List<MyThread> _threads = new Vector<MyThread>();
    private final BlockingQueue<T> _queue;
    private final MyThreadGroup _myThreadGroup;
}
