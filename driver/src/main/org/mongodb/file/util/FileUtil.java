package org.mongodb.file.util;

import java.io.File;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public final class FileUtil {

    private FileUtil() {
        // hidden
    }

    public static String getExtension(final File f) {

        return getExtension(f.getName());
    }

    public static String getExtension(final String f) {

        return f.substring(f.lastIndexOf('.') + 1);
    }

    /**
     * Copied from the old Hex.java, needed for MD5 handling
     * 
     * @param bytes
     * @return the hex string
     */
    public static String toHex(final byte[] bytes) {
        StringBuilder sb = new StringBuilder();

        for (final byte b : bytes) {
            String s = Integer.toHexString(0xff & b);

            if (s.length() < 2) {
                sb.append("0");
            }
            sb.append(s);
        }

        return sb.toString();

    }

    /**
     * Produce hex representation of the MD5 digest of a byte array
     * 
     * @param data
     *            bytes to digest
     * @return hex string of the MD5 digest
     */
    public static String hexMD5(final byte[] data) {

        try {
            MessageDigest md5 = MessageDigest.getInstance("MD5");

            md5.reset();
            md5.update(data);
            byte[] digest = md5.digest();

            return toHex(digest);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("Error - this implementation of Java doesn't support MD5.");
        }
    }

}
