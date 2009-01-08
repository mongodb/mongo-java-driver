// UniqueListTest.java

package com.mongodb.util;

import org.testng.annotations.Test;

public class UniqueListTest extends com.mongodb.util.TestCase {

    @org.testng.annotations.Test
    public void test1(){
        UniqueList l = new UniqueList();
        l.add( "a" );
        assertEquals( 1 , l.size() );
        l.add( "a" );
        assertEquals( 1 , l.size() );
        l.add( "b" );
        assertEquals( 2 , l.size() );
    }
    
    public static void main( String args[] ){
        (new UniqueListTest()).runConsole();
    }
}
