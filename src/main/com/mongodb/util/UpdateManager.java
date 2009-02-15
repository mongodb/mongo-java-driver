// UpdateManager.java

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

import java.util.*;
import java.lang.ref.*;

public class UpdateManager {
    
    public static void add( Updatable u ){
	_thingsToUpdate.add( new UpdateThing( u ) );
    }
    
    static class UpdateThing extends WeakReference<Updatable> {
	UpdateThing( Updatable u ){
	    super( u );
	    _lastUpdate = System.currentTimeMillis();
	}
	
	boolean dead(){
	    return get() == null;
	}

	boolean needsUpdate( final long now ){
	    Updatable u = get();
	    if ( u == null )
		return false;
	    return u.timeBeforeUpdates() + _lastUpdate < now;
	}
	
	void update(){
	    _lastUpdate = System.currentTimeMillis();
	    Updatable u = get();
	    if ( u == null )
		return;
	    
	    u.update();
	}

	long _lastUpdate;
    }
    
    static class UpdateThread extends Thread {
	UpdateThread(){
	    super( "UpdateThread" );
	    setDaemon( true );
	    start();
	}
	
	public void run(){
	    while( true ){
		final long now = System.currentTimeMillis();
		try {
		    int max = _thingsToUpdate.size();
		    for ( int i=0; i<_thingsToUpdate.size(); i++){
			UpdateThing ut = _thingsToUpdate.get( i );
			if ( ut.dead() ){
			    _thingsToUpdate.remove( i );
			    i--;
			    continue;
			}
			
			if ( ! ut.needsUpdate( now ) )
			    continue;

			try {
			    ut.update();
			}
			catch ( Exception e ){
			    e.printStackTrace();
			}
		    }
		}
		catch ( Exception e ){
		    e.printStackTrace();
		}
		
		ThreadUtil.sleep( 1000 * 2 );
	    }
	}
    }

    private static final List<UpdateThing> _thingsToUpdate = Collections.synchronizedList( new ArrayList<UpdateThing>() );
    private static final UpdateThread _thread = new UpdateThread();
}
