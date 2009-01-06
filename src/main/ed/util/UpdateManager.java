// UpdateManager.java

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
