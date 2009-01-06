// TimeConstants.java

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

public class TimeConstants {
    
    public static final long MS_MILLISECOND = 1;
    public static final long MS_SECOND = 1000;
    public static final long MS_MINUTE = MS_SECOND * 60;
    public static final long MS_HOUR = MS_MINUTE * 60;
    public static final long MS_DAY = MS_HOUR * 24;
    public static final long MS_WEEK = MS_DAY * 7;    
    public static final long MS_MONTH = MS_WEEK * 4;    
    public static final long MS_YEAR = MS_DAY * 365;

    public static final long S_SECOND = 1;
    public static final long S_MINUTE = 60 * S_SECOND;
    public static final long S_HOUR = 60 * S_MINUTE;
    public static final long S_DAY = 24 * S_HOUR;
    public static final long S_WEEK = 7 * S_DAY;
    public static final long S_MONTH = 30 * S_DAY;
    public static final long S_YEAR = S_DAY * 365;
}
