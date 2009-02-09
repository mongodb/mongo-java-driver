/**
 *  See the NOTICE.txt file distributed with this work for
 *  information regarding copyright ownership.
 *
 *  The authors license this file to you under the
 *  Apache License, Version 2.0 (the "License"); you may not use
 *  this file except in compliance with the License.  You may
 *  obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.mongodb.util;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 *  Misc utility helpers.  Not sure what else to call the class
 */
public class Util {

    /**
     *  Produce hex representation of the MD5 digest of a byte array
     *
     * @param data bytes to digest
     * @return hex string of the MD5 digest
     */
    public static String hexMD5(byte[] data) {

        try {
            MessageDigest md5 = MessageDigest.getInstance("MD5");

            md5.reset();
            md5.update(data);
            byte digest[] = md5.digest();

            StringBuffer sb = new StringBuffer();

            for (byte aDigest : digest) {
                String s = Integer.toHexString(0xff & aDigest);

                if (s.length() < 2) {
                    sb.append("0");
                }
                sb.append(s);
            }

            return sb.toString();

        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("Error - this implementation of Java doesn't support MD5.");
        }
    }
}
