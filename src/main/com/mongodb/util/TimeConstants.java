// TimeConstants.java

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

/**
 * @deprecated This class is NOT a part of public API and will be dropped in 3.x versions.
 */
@Deprecated
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
