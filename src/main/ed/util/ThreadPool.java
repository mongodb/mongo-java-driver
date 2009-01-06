// ThreadPool.java

/**
*    Copyright (C) 2008 10gen Inc.
*  
*    This program is free software: you can redistribute it and/or  modify
*    it under the terms of the GNU Affero General Public License, version 3,
*    as published by the Free Software Foundation.
*  
*    This program is distributed in the hope that it will be useful,
*    but WITHOUT ANY WARRANTY; without even the implied warranty of
*    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
*    GNU Affero General Public License for more details.
*  
*    You should have received a copy of the GNU Affero General Public License
*    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

package ed.util;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/** @expose */
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
            System.err.println( "ThreadPool [" + _name + "] uncaughtException" );
            e.printStackTrace();
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
