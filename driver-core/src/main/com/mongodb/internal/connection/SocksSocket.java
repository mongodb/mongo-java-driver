/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.mongodb.internal.connection;

import com.mongodb.connection.ProxySettings;
import com.mongodb.internal.Timeout;
import com.mongodb.internal.VisibleForTesting;
import com.mongodb.lang.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ConnectException;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.assertNotNull;
import static com.mongodb.assertions.Assertions.assertTrue;
import static com.mongodb.assertions.Assertions.fail;

/**
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public final class SocksSocket extends Socket {
    private static final byte LENGTH_OF_IPV4 = 4;
    private static final byte LENGTH_OF_IPV6 = 16;
    private static final byte SOCKS_VERSION = 5;
    private static final byte RESERVED = 0x00;
    private static final byte PORT_SIZE = 2;
    private static final byte AUTHENTICATION_SUCCEEDED_STATUS = 0x00;
    private static final byte REQUEST_OK = 0;
    private static final byte ADDRESS_TYPE_DOMAIN_NAME = 3;
    private static final byte ADDRESS_TYPE_IPV4 = 1;
    private static final byte ADDRESS_TYPE_IPV6 = 4;
    private static final int DEFAULT_PORT = 1080;
    private final SocksAuthenticationMethod[] authenticationMethods;
    private final InetSocketAddress proxyAddress;
    private InetSocketAddress remoteAddress;
    @Nullable
    private String proxyUsername;
    @Nullable
    private String proxyPassword;
    @Nullable
    private final Socket socket;
    private InputStream inputStream;
    private OutputStream outputStream;


    public SocksSocket(final ProxySettings proxySettings) {
        this(null, proxySettings);
    }

    public SocksSocket(@Nullable final Socket socket, final ProxySettings proxySettings) {
        int port = getPort(proxySettings);
        assertTrue(proxySettings.getHost() != null);
        assertTrue(port >= 0);

        this.proxyAddress = new InetSocketAddress(proxySettings.getHost(), port);
        this.socket = socket;

        if (proxySettings.getUsername() != null && proxySettings.getPassword() != null) {
            this.authenticationMethods = new SocksAuthenticationMethod[]{
                    SocksAuthenticationMethod.NO_AUTH,
                    SocksAuthenticationMethod.USERNAME_PASSWORD};
            this.proxyUsername = proxySettings.getUsername();
            this.proxyPassword = proxySettings.getPassword();
        } else {
            this.authenticationMethods = new SocksAuthenticationMethod[]{SocksAuthenticationMethod.NO_AUTH};
        }
    }

    private static int getPort(final ProxySettings proxySettings) {
        if (proxySettings.getPort() != null) {
            return proxySettings.getPort();
        }
        return DEFAULT_PORT;
    }

    @Override
    public void connect(final SocketAddress endpoint) throws IOException {
        connect(endpoint, 0);
    }

    @Override
    public void connect(final SocketAddress endpoint, final int timeoutMs) throws IOException {
        try {
            Timeout timeout = toTimeout(timeoutMs);
            InetSocketAddress unresolvedAddress = (InetSocketAddress) endpoint;
            assertTrue(unresolvedAddress.isUnresolved());
            this.remoteAddress = unresolvedAddress;
            if (socket != null && !socket.isConnected()) {
                socket.connect(proxyAddress, remainingMillis(timeout));
                inputStream = socket.getInputStream();
                outputStream = socket.getOutputStream();
            } else {
                super.connect(proxyAddress, remainingMillis(timeout));
                inputStream = getInputStream();
                outputStream = getOutputStream();
            }
            SocksAuthenticationMethod authenticationMethod = performHandshake(timeout);
            authenticate(authenticationMethod, timeout);
            sendConnect(timeout);
        } catch (SocketException socketException) {
            close();
            throw socketException;
        }
    }

    private void sendConnect(final Timeout timeout) throws IOException {
        String host = remoteAddress.getHostName();
        int port = remoteAddress.getPort();
        byte[] bytesOfHost = host.getBytes(StandardCharsets.ISO_8859_1);
        final int hostLength = host.length();

        byte[] bufferSent;
        byte[] ipAddress = createByteArrayFromIpAddress(remoteAddress);
        byte addressType = determineAddressType(ipAddress);
        bufferSent = createBuffer(addressType, hostLength);

        bufferSent[0] = SOCKS_VERSION;
        bufferSent[1] = (byte) SocksCommand.CONNECT.getCommandNumber();
        bufferSent[2] = RESERVED;
        switch (addressType) {
            case ADDRESS_TYPE_DOMAIN_NAME:
                bufferSent[3] = ADDRESS_TYPE_DOMAIN_NAME;
                bufferSent[4] = (byte) hostLength;
                System.arraycopy(bytesOfHost, 0, bufferSent, 5, hostLength);
                bufferSent[5 + host.length()] = (byte) ((port & 0xff00) >> 8);
                bufferSent[6 + host.length()] = (byte) (port & 0xff);
                break;
            case ADDRESS_TYPE_IPV4:
                bufferSent[3] = ADDRESS_TYPE_IPV4;
                System.arraycopy(ipAddress, 0, bufferSent, 4, ipAddress.length);
                bufferSent[4 + ipAddress.length] = (byte) ((port & 0xff00) >> 8);
                bufferSent[5 + ipAddress.length] = (byte) (port & 0xff);
                break;
            case ADDRESS_TYPE_IPV6:
                bufferSent[3] = ADDRESS_TYPE_IPV6;
                System.arraycopy(ipAddress, 0, bufferSent, 4, ipAddress.length);
                bufferSent[4 + ipAddress.length] = (byte) ((port & 0xff00) >> 8);
                bufferSent[5 + ipAddress.length] = (byte) (port & 0xff);
                break;
            default:
                fail();
        }
        outputStream.write(bufferSent);
        outputStream.flush();
        checkServerReply(timeout);
    }

    @Nullable
    private static byte[] createByteArrayFromIpAddress(final InetSocketAddress remoteAddress) throws SocketException, UnknownHostException {
        InetAddress inetAddress = identifyAddressType(remoteAddress);
        if (inetAddress instanceof Inet4Address) {
            return inetAddress.getAddress();
        }
        if (inetAddress instanceof Inet6Address) {
            return inetAddress.getAddress();
        }
        return null;
    }

    @Nullable
    @VisibleForTesting(otherwise = VisibleForTesting.AccessModifier.PRIVATE)
    public static InetAddress identifyAddressType(final InetSocketAddress remoteAddress) throws SocketException, UnknownHostException {
        String host = remoteAddress.getHostName();
        if (host.contains(":") || (host.contains(".") && !hasAlphabeticCharacters(host))) {
            try {
                return InetAddress.getByName(host);
            } catch (UnknownHostException e) {
                //invalid IP address
            }
        }
        return null;
    }

    private static boolean hasAlphabeticCharacters(final String input) {
        for (int i = 0; i < input.length(); i++) {
            if (Character.isAlphabetic(input.charAt(i))) {
                return true;
            }
        }
        return false;
    }

    private byte determineAddressType(@Nullable final byte[] ipAddress) {
        if (ipAddress == null) {
            return ADDRESS_TYPE_DOMAIN_NAME;
        } else if (ipAddress.length == LENGTH_OF_IPV4) {
            return ADDRESS_TYPE_IPV4;
        }
        return ADDRESS_TYPE_IPV6;
    }

    private static byte[] createBuffer(final byte addressType, final int hostLength) {
        switch (addressType) {
            case ADDRESS_TYPE_DOMAIN_NAME:
                return new byte[7 + hostLength];
            case ADDRESS_TYPE_IPV4:
                return new byte[6 + LENGTH_OF_IPV4];
            case ADDRESS_TYPE_IPV6:
                return new byte[6 + LENGTH_OF_IPV6];
            default:
                break;
        }
        throw fail();
    }

    private void checkServerReply(final Timeout timeout) throws IOException {
        byte[] data = readSocksReply(inputStream, 4, timeout);
        byte status = data[1];

        if (status == REQUEST_OK) {
            switch (data[3]) {
                case ADDRESS_TYPE_IPV4:
                    readSocksReply(inputStream, 4 + PORT_SIZE, timeout);
                    break;
                case ADDRESS_TYPE_DOMAIN_NAME:
                    byte hostNameLength = readSocksReply(inputStream, 1, timeout)[0];
                    readSocksReply(inputStream, hostNameLength + PORT_SIZE, timeout);
                    break;
                case ADDRESS_TYPE_IPV6:
                    readSocksReply(inputStream, 16 + PORT_SIZE, timeout);
                    break;
                default:
                    throw new ConnectException("Reply from SOCKS proxy server contains wrong code");
            }
            return;
        }

        for (ServerErrorReply serverErrorReply : ServerErrorReply.values()) {
            if (status == serverErrorReply.getReplyNumber()) {
                throw new ConnectException(serverErrorReply.getMessage());
            }
        }
        throw new ConnectException("Unknown status");
    }

    private void authenticate(final SocksAuthenticationMethod authenticationMethod, final Timeout timeout) throws IOException {
        if (authenticationMethod == SocksAuthenticationMethod.USERNAME_PASSWORD) {
            final byte[] bytesOfUsername = assertNotNull(proxyUsername).getBytes(StandardCharsets.ISO_8859_1);
            final byte[] bytesOfPassword = assertNotNull(proxyPassword).getBytes(StandardCharsets.ISO_8859_1);
            final int usernameLength = bytesOfUsername.length;
            final int passwordLength = bytesOfPassword.length;
            final byte[] command = new byte[3 + usernameLength + passwordLength];

            command[0] = 0x01;
            command[1] = (byte) usernameLength;
            System.arraycopy(bytesOfUsername, 0, command, 2, usernameLength);
            command[2 + usernameLength] = (byte) passwordLength;
            System.arraycopy(bytesOfPassword, 0, command, 3 + usernameLength,
                    passwordLength);
            outputStream.write(command);
            outputStream.flush();

            byte[] authenticationResult = readSocksReply(inputStream, 2, timeout);

            if (authenticationResult[1] != AUTHENTICATION_SUCCEEDED_STATUS) {
                  /* RFC 1929 specifies that the connection MUST be closed if
                   authentication fails */
                throw new ConnectException("Authentication failed");
            }
        }
    }

    private SocksAuthenticationMethod performHandshake(final Timeout timeout) throws IOException {
        int methodsCount = authenticationMethods.length;

        byte[] bufferSent = new byte[2 + methodsCount];
        bufferSent[0] = SOCKS_VERSION;
        bufferSent[1] = (byte) methodsCount;
        for (int i = 0; i < methodsCount; i++) {
            bufferSent[2 + i] = (byte) authenticationMethods[i].getMethodNumber();
        }
        outputStream.write(bufferSent);
        outputStream.flush();

        byte[] handshakeReply = readSocksReply(inputStream, 2, timeout);

        if (handshakeReply[0] != SOCKS_VERSION) {
            throw new ConnectException("Remote server doesn't support SOCKS5");
        }
        if (handshakeReply[1] == (byte) 0xFF) {
            throw new ConnectException("None of the authentication methods listed are acceptable");
        }
        if (handshakeReply[1] == SocksAuthenticationMethod.NO_AUTH.getMethodNumber()) {
            return SocksAuthenticationMethod.NO_AUTH;
        }

        return SocksAuthenticationMethod.USERNAME_PASSWORD;
    }

    private static Timeout toTimeout(final int timeoutMs) {
        if (timeoutMs == 0) {
            return Timeout.infinite();
        }
        return Timeout.startNow(timeoutMs, TimeUnit.MILLISECONDS);
    }

    private static int remainingMillis(final Timeout timeout) throws IOException {
        if (timeout.isInfinite()) {
            return 0;
        }

        final int remaining = (int) timeout.remaining(TimeUnit.MILLISECONDS);
        if (remaining > 0) {
            return remaining;
        }

        throw new SocketTimeoutException("Socket connection timed out");
    }

    private byte[] readSocksReply(final InputStream in, final int length, final Timeout timeout) throws IOException {
        byte[] data = new byte[length];
        int received = 0;
        int originalTimeout = getSoTimeout();
        try {
            while (received < length) {
                int count;
                int remaining = remainingMillis(timeout);
                setSoTimeout(remaining);
                try {
                    count = in.read(data, received, length - received);
                } catch (SocketTimeoutException e) {
                    throw new SocketTimeoutException("Socket connection timed out");
                }
                if (count < 0) {
                    throw new ConnectException("Malformed reply from SOCKS proxy server");
                }
                received += count;
            }
        } finally {
            setSoTimeout(originalTimeout);
        }
        return data;
    }

    public static byte[] read(final InputStream inputStream, final int length) throws IOException {
        byte[] bytes = new byte[length];
        for (int i = 0; i < length; i++) {
            int read = inputStream.read();
            if (read < 0) {
                throw new ConnectException("End of stream");
            }
            bytes[i] = (byte) read;
        }
        return bytes;
    }

    @Override
    public synchronized void close() throws IOException {
        if (socket != null) {
            socket.close();
        } else {
            super.close();
        }
    }

    @Override
    public synchronized void setSoTimeout(final int timeout) throws SocketException {
        if (socket != null) {
            socket.setSoTimeout(timeout);
        } else {
            super.setSoTimeout(timeout);
        }
    }

    @Override
    public InputStream getInputStream() throws IOException {
        if (socket != null) {
            return socket.getInputStream();
        }
        return super.getInputStream();
    }

    @Override
    public OutputStream getOutputStream() throws IOException {
        if (socket != null) {
            return socket.getOutputStream();
        }
        return super.getOutputStream();
    }

    private enum SocksCommand {

        CONNECT(0x01);

        private final int value;

        SocksCommand(final int value) {
            this.value = value;
        }

        public int getCommandNumber() {
            return value;
        }
    }

    private enum SocksAuthenticationMethod {
        NO_AUTH(0x00),
        USERNAME_PASSWORD(0x02);

        private final int methodNumber;

        SocksAuthenticationMethod(final int methodNumber) {
            this.methodNumber = methodNumber;
        }

        public int getMethodNumber() {
            return methodNumber;
        }

    }

    private enum ServerErrorReply {
        GENERAL_FAILURE(1, "Remote server doesn't support SOCKS5"),
        NOT_ALLOWED(2, "Proxy server general failure"),
        NET_UNREACHABLE(3, "Connection not allowed by ruleset"),
        HOST_UNREACHABLE(4, "Network is unreachable"),
        CONN_REFUSED(5, "Host is unreachable"),
        TTL_EXPIRED(6, "Connection has been refused"),
        CMD_NOT_SUPPORTED(7, "TTL expired"),
        ADDR_TYPE_NOT_SUP(8, "Address type not supported");

        private final int replyNumber;
        private final String message;

        ServerErrorReply(final int replyNumber, final String message) {
            this.replyNumber = replyNumber;
            this.message = message;
        }

        public int getReplyNumber() {
            return replyNumber;
        }

        public String getMessage() {
            return message;
        }
    }
}
