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
import com.mongodb.internal.time.Timeout;
import com.mongodb.lang.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.nio.channels.SocketChannel;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static com.mongodb.assertions.Assertions.assertFalse;
import static com.mongodb.assertions.Assertions.assertNotNull;
import static com.mongodb.assertions.Assertions.assertTrue;
import static com.mongodb.assertions.Assertions.fail;
import static com.mongodb.assertions.Assertions.isTrueArgument;
import static com.mongodb.internal.connection.DomainNameUtils.isDomainName;
import static com.mongodb.internal.connection.SocksSocket.AddressType.DOMAIN_NAME;
import static com.mongodb.internal.connection.SocksSocket.AddressType.IP_V4;
import static com.mongodb.internal.connection.SocksSocket.AddressType.IP_V6;
import static com.mongodb.internal.connection.SocksSocket.ServerReply.REPLY_SUCCEEDED;

/**
 * <p>This class is not part of the public API and may be removed or changed at any time</p>
 */
public final class SocksSocket extends Socket {
    private static final byte SOCKS_VERSION = 0x05;
    private static final byte RESERVED = 0x00;
    private static final byte PORT_LENGTH = 2;
    private static final byte AUTHENTICATION_SUCCEEDED_STATUS = 0x00;
    public static final String IP_PARSING_ERROR_SUFFIX = " is not an IP string literal";
    private static final byte USER_PASSWORD_SUB_NEGOTIATION_VERSION = 0x01;
    private InetSocketAddress remoteAddress;
    private final ProxySettings proxySettings;
    @Nullable
    private final Socket socket;

    public SocksSocket(final ProxySettings proxySettings) {
        this(null, proxySettings);
    }

    public SocksSocket(@Nullable final Socket socket, final ProxySettings proxySettings) {
        assertNotNull(proxySettings.getHost());
        /* Explanation for using Socket instead of SocketFactory: The process of initializing a socket for a SOCKS proxy follows a specific sequence.
           First, a basic TCP socket is created using the socketFactory, and then it's customized with settings.
           Subsequently, the socket is wrapped within a SocksSocket instance to provide additional functionality.
           Due to limitations in extending methods within SocksSocket for Java 11, the configuration step must precede the wrapping stage.
           As a result, passing SocketFactory directly into this constructor for socket creation is not feasible.
        */
        if (socket != null) {
            assertFalse(socket.isConnected());
        }
        this.socket = socket;
        this.proxySettings = proxySettings;
    }

    @Override
    public void connect(final SocketAddress endpoint, final int timeoutMs) throws IOException {
        // `Socket` requires `IllegalArgumentException`
        isTrueArgument("timeoutMs", timeoutMs >= 0);
        try {
            Timeout timeout = toTimeout(timeoutMs);
            InetSocketAddress unresolvedAddress = (InetSocketAddress) endpoint;
            assertTrue(unresolvedAddress.isUnresolved());
            this.remoteAddress = unresolvedAddress;

            InetSocketAddress proxyAddress = new InetSocketAddress(assertNotNull(proxySettings.getHost()), proxySettings.getPort());
            if (socket != null) {
                socket.connect(proxyAddress, remainingMillis(timeout));
            } else {
                super.connect(proxyAddress, remainingMillis(timeout));
            }
            SocksAuthenticationMethod authenticationMethod = performNegotiation(timeout);
            authenticate(authenticationMethod, timeout);
            sendConnect(timeout);
        } catch (SocketException socketException) {
            /*
             * The 'close()' call here has two purposes:
             *
             * 1. Enforces self-closing under RFC 1928 if METHOD is X'FF'.
             * 2. Handles all other errors during connection, distinct from external closures.
             */
            close();
            throw socketException;
        }
    }

    private void sendConnect(final Timeout timeout) throws IOException {
        final String host = remoteAddress.getHostName();
        final int port = remoteAddress.getPort();
        final byte[] bytesOfHost = host.getBytes(StandardCharsets.US_ASCII);
        final int hostLength = bytesOfHost.length;

        AddressType addressType;
        byte[] ipAddress = null;
        if (isDomainName(host)) {
            addressType = DOMAIN_NAME;
        } else {
            ipAddress = createByteArrayFromIpAddress(host);
            addressType = determineAddressType(ipAddress);
        }
        byte[] bufferSent = createBuffer(addressType, hostLength);
        bufferSent[0] = SOCKS_VERSION;
        bufferSent[1] = SocksCommand.CONNECT.getCommandNumber();
        bufferSent[2] = RESERVED;
        switch (addressType) {
            case DOMAIN_NAME:
                bufferSent[3] = DOMAIN_NAME.getAddressTypeNumber();
                bufferSent[4] = (byte) hostLength;
                System.arraycopy(bytesOfHost, 0, bufferSent, 5, hostLength);
                addPort(bufferSent, 5 + hostLength, port);
                break;
            case IP_V4:
                bufferSent[3] = IP_V4.getAddressTypeNumber();
                System.arraycopy(ipAddress, 0, bufferSent, 4, ipAddress.length);
                addPort(bufferSent, 4 + ipAddress.length, port);
                break;
            case IP_V6:
                bufferSent[3] = DOMAIN_NAME.getAddressTypeNumber();
                System.arraycopy(ipAddress, 0, bufferSent, 4, ipAddress.length);
                addPort(bufferSent, 4 + ipAddress.length, port);
                break;
            default:
                fail();
        }
        OutputStream outputStream = getOutputStream();
        outputStream.write(bufferSent);
        outputStream.flush();
        checkServerReply(timeout);
    }

    private static void addPort(final byte[] bufferSent, final int index, final int port) {
        bufferSent[index] = (byte) (port >> 8);
        bufferSent[index + 1] = (byte) port;
    }

    private static byte[] createByteArrayFromIpAddress(final String host) throws SocketException {
        byte[] bytes = InetAddressUtils.ipStringToBytes(host);
        if (bytes == null) {
            throw new SocketException(host + IP_PARSING_ERROR_SUFFIX);
        }
        return bytes;
    }

    private AddressType determineAddressType(final byte[] ipAddress) {
        if (ipAddress.length == IP_V4.getLength()) {
            return IP_V4;
        } else if (ipAddress.length == IP_V6.getLength()) {
            return IP_V6;
        }
        throw fail();
    }

    private static byte[] createBuffer(final AddressType addressType, final int hostLength) {
        switch (addressType) {
            case DOMAIN_NAME:
                return new byte[7 + hostLength];
            case IP_V4:
                return new byte[6 + IP_V4.getLength()];
            case IP_V6:
                return new byte[6 + IP_V6.getLength()];
            default:
                throw fail();
        }
    }

    private void checkServerReply(final Timeout timeout) throws IOException {
        byte[] data = readSocksReply(4, timeout);
        ServerReply reply = ServerReply.of(data[1]);
        if (reply == REPLY_SUCCEEDED) {
            switch (AddressType.of(data[3])) {
                case DOMAIN_NAME:
                    byte hostNameLength = readSocksReply(1, timeout)[0];
                    readSocksReply(hostNameLength + PORT_LENGTH, timeout);
                    break;
                case IP_V4:
                    readSocksReply(IP_V4.getLength() + PORT_LENGTH, timeout);
                    break;
                case IP_V6:
                    readSocksReply(IP_V6.getLength() + PORT_LENGTH, timeout);
                    break;
                default:
                    throw fail();
            }
            return;
        }
        throw new ConnectException(reply.getMessage());
    }

    private void authenticate(final SocksAuthenticationMethod authenticationMethod, final Timeout timeout) throws IOException {
        if (authenticationMethod == SocksAuthenticationMethod.USERNAME_PASSWORD) {
            final byte[] bytesOfUsername = assertNotNull(proxySettings.getUsername()).getBytes(StandardCharsets.UTF_8);
            final byte[] bytesOfPassword = assertNotNull(proxySettings.getPassword()).getBytes(StandardCharsets.UTF_8);
            final int usernameLength = bytesOfUsername.length;
            final int passwordLength = bytesOfPassword.length;
            final byte[] command = new byte[3 + usernameLength + passwordLength];

            command[0] = USER_PASSWORD_SUB_NEGOTIATION_VERSION;
            command[1] = (byte) usernameLength;
            System.arraycopy(bytesOfUsername, 0, command, 2, usernameLength);
            command[2 + usernameLength] = (byte) passwordLength;
            System.arraycopy(bytesOfPassword, 0, command, 3 + usernameLength,
                    passwordLength);

            OutputStream outputStream = getOutputStream();
            outputStream.write(command);
            outputStream.flush();

            byte[] authResult = readSocksReply(2, timeout);
            byte authStatus = authResult[1];

            if (authStatus != AUTHENTICATION_SUCCEEDED_STATUS) {
                throw new ConnectException("Authentication failed. Proxy server returned status: " + authStatus);
            }
        }
    }

    private SocksAuthenticationMethod performNegotiation(final Timeout timeout) throws IOException {
        SocksAuthenticationMethod[] authenticationMethods = getSocksAuthenticationMethods();

        int methodsCount = authenticationMethods.length;

        byte[] bufferSent = new byte[2 + methodsCount];
        bufferSent[0] = SOCKS_VERSION;
        bufferSent[1] = (byte) methodsCount;
        for (int i = 0; i < methodsCount; i++) {
            bufferSent[2 + i] = authenticationMethods[i].getMethodNumber();
        }

        OutputStream outputStream = getOutputStream();
        outputStream.write(bufferSent);
        outputStream.flush();

        byte[] handshakeReply = readSocksReply(2, timeout);

        if (handshakeReply[0] != SOCKS_VERSION) {
            throw new ConnectException("Remote server doesn't support socks version 5"
                    + " Received version: " + handshakeReply[0]);
        }
        byte authMethodNumber = handshakeReply[1];
        if (authMethodNumber == (byte) 0xFF) {
            throw new ConnectException("None of the authentication methods listed are acceptable. Attempted methods: "
                    + Arrays.toString(authenticationMethods));
        }
        if (authMethodNumber == SocksAuthenticationMethod.NO_AUTH.getMethodNumber()) {
            return SocksAuthenticationMethod.NO_AUTH;
        } else if (authMethodNumber == SocksAuthenticationMethod.USERNAME_PASSWORD.getMethodNumber()) {
            return SocksAuthenticationMethod.USERNAME_PASSWORD;
        }

        throw new ConnectException("Proxy returned unsupported authentication method: " + authMethodNumber);
    }

    private SocksAuthenticationMethod[] getSocksAuthenticationMethods() {
        SocksAuthenticationMethod[] authMethods;
        if (proxySettings.getUsername() != null) {
            authMethods = new SocksAuthenticationMethod[]{
                    SocksAuthenticationMethod.NO_AUTH,
                    SocksAuthenticationMethod.USERNAME_PASSWORD};
        } else {
            authMethods = new SocksAuthenticationMethod[]{SocksAuthenticationMethod.NO_AUTH};
        }
        return authMethods;
    }

    private static Timeout toTimeout(final int timeoutMs) {
        if (timeoutMs == 0) {
            return Timeout.infinite();
        }
        return Timeout.expiresIn(timeoutMs, TimeUnit.MILLISECONDS);
    }

    private static int remainingMillis(final Timeout timeout) throws IOException {
        if (timeout.isInfinite()) {
            return 0;
        }

        final int remaining = Math.toIntExact(timeout.remaining(TimeUnit.MILLISECONDS));
        if (remaining > 0) {
            return remaining;
        }

        throw new SocketTimeoutException("Socket connection timed out");
    }

    private byte[] readSocksReply(final int length, final Timeout timeout) throws IOException {
        InputStream inputStream = getInputStream();
        byte[] data = new byte[length];
        int received = 0;
        int originalTimeout = getSoTimeout();
        try {
            while (received < length) {
                int count;
                int remaining = remainingMillis(timeout);
                setSoTimeout(remaining);
                count = inputStream.read(data, received, length - received);
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

    enum SocksCommand {

        CONNECT(0x01);

        private final byte value;

        SocksCommand(final int value) {
            this.value = (byte) value;
        }

        public byte getCommandNumber() {
            return value;
        }
    }

    private enum SocksAuthenticationMethod {
        NO_AUTH(0x00),
        USERNAME_PASSWORD(0x02);

        private final byte methodNumber;

        SocksAuthenticationMethod(final int methodNumber) {
            this.methodNumber = (byte) methodNumber;
        }

        public byte getMethodNumber() {
            return methodNumber;
        }
    }

    enum AddressType {
        IP_V4(0x01, 4),
        IP_V6(0x04, 16),
        DOMAIN_NAME(0x03, -1) {
            public byte getLength() {
                throw fail();
            }
        };

        private final byte length;
        private final byte addressTypeNumber;

        AddressType(final int addressTypeNumber, final int length) {
            this.addressTypeNumber = (byte) addressTypeNumber;
            this.length = (byte) length;
        }

        static AddressType of(final byte signedAddressType) throws ConnectException {
            int addressTypeNumber = Byte.toUnsignedInt(signedAddressType);
            for (AddressType addressType : AddressType.values()) {
                if (addressTypeNumber == addressType.getAddressTypeNumber()) {
                    return addressType;
                }
            }
            throw new ConnectException("Reply from SOCKS proxy server contains wrong address type"
                    + " Address type: " + addressTypeNumber);
        }

        byte getLength() {
            return length;
        }

        byte getAddressTypeNumber() {
            return addressTypeNumber;
        }

    }

    enum ServerReply {
        REPLY_SUCCEEDED(0x00, "Succeeded"),
        GENERAL_FAILURE(0x01, "General SOCKS5 server failure"),
        NOT_ALLOWED(0x02, "Connection is not allowed by ruleset"),
        NET_UNREACHABLE(0x03, "Network is unreachable"),
        HOST_UNREACHABLE(0x04, "Host is unreachable"),
        CONN_REFUSED(0x05, "Connection has been refused"),
        TTL_EXPIRED(0x06, "TTL is expired"),
        CMD_NOT_SUPPORTED(0x07, "Command is not supported"),
        ADDR_TYPE_NOT_SUP(0x08, "Address type is not supported");

        private final int replyNumber;
        private final String message;

        ServerReply(final int replyNumber, final String message) {
            this.replyNumber = replyNumber;
            this.message = message;
        }

        static ServerReply of(final byte byteStatus) throws ConnectException {
            int status = Byte.toUnsignedInt(byteStatus);
            for (ServerReply serverReply : ServerReply.values()) {
                if (status == serverReply.replyNumber) {
                    return serverReply;
                }
            }

            throw new ConnectException("Unknown reply field. Reply field: " + status);
        }

        public String getMessage() {
            return message;
        }
    }

    @Override
    @SuppressWarnings("try")
    public void close() throws IOException {
        /*
          If this.socket is not null, this class essentially acts as a wrapper and we neither bind nor connect in the superclass,
          nor do we get input/output streams from the superclass. While it might seem reasonable to skip calling super.close() in this case,
          the Java SE Socket documentation doesn't definitively clarify this. Therefore, it's safer to always call super.close().
         */
        try (Socket autoClosed = socket) {
            super.close();
        }
    }

    @Override
    public void setSoTimeout(final int timeout) throws SocketException {
        if (socket != null) {
            socket.setSoTimeout(timeout);
        } else {
            super.setSoTimeout(timeout);
        }
    }

    @Override
    public int getSoTimeout() throws SocketException {
        if (socket != null) {
            return socket.getSoTimeout();
        } else {
            return super.getSoTimeout();
        }
    }

    @Override
    public void bind(final SocketAddress bindpoint) throws IOException {
        if (socket != null) {
            socket.bind(bindpoint);
        } else {
            super.bind(bindpoint);
        }
    }

    @Override
    public InetAddress getInetAddress() {
        if (socket != null) {
            return socket.getInetAddress();
        } else {
            return super.getInetAddress();
        }
    }

    @Override
    public InetAddress getLocalAddress() {
        if (socket != null) {
            return socket.getLocalAddress();
        } else {
            return super.getLocalAddress();
        }
    }

    @Override
    public int getPort() {
        if (socket != null) {
            return socket.getPort();
        } else {
            return super.getPort();
        }
    }

    @Override
    public int getLocalPort() {
        if (socket != null) {
            return socket.getLocalPort();
        } else {
            return super.getLocalPort();
        }
    }

    @Override
    public SocketAddress getRemoteSocketAddress() {
        if (socket != null) {
            return socket.getRemoteSocketAddress();
        } else {
            return super.getRemoteSocketAddress();
        }
    }

    @Override
    public SocketAddress getLocalSocketAddress() {
        if (socket != null) {
            return socket.getLocalSocketAddress();
        } else {
            return super.getLocalSocketAddress();
        }
    }

    @Override
    public SocketChannel getChannel() {
        if (socket != null) {
            return socket.getChannel();
        } else {
            return super.getChannel();
        }
    }

    @Override
    public void setTcpNoDelay(final boolean on) throws SocketException {
        if (socket != null) {
            socket.setTcpNoDelay(on);
        } else {
            super.setTcpNoDelay(on);
        }
    }

    @Override
    public boolean getTcpNoDelay() throws SocketException {
        if (socket != null) {
            return socket.getTcpNoDelay();
        } else {
            return super.getTcpNoDelay();
        }
    }

    @Override
    public void setSoLinger(final boolean on, final int linger) throws SocketException {
        if (socket != null) {
            socket.setSoLinger(on, linger);
        } else {
            super.setSoLinger(on, linger);
        }
    }

    @Override
    public int getSoLinger() throws SocketException {
        if (socket != null) {
            return socket.getSoLinger();
        } else {
            return super.getSoLinger();
        }
    }

    @Override
    public void sendUrgentData(final int data) throws IOException {
        if (socket != null) {
            socket.sendUrgentData(data);
        } else {
            super.sendUrgentData(data);
        }
    }

    @Override
    public void setOOBInline(final boolean on) throws SocketException {
        if (socket != null) {
            socket.setOOBInline(on);
        } else {
            super.setOOBInline(on);
        }
    }

    @Override
    public boolean getOOBInline() throws SocketException {
        if (socket != null) {
            return socket.getOOBInline();
        } else {
            return super.getOOBInline();
        }
    }

    @Override
    public void setSendBufferSize(final int size) throws SocketException {
        if (socket != null) {
            socket.setSendBufferSize(size);
        } else {
            super.setSendBufferSize(size);
        }
    }

    @Override
    public int getSendBufferSize() throws SocketException {
        if (socket != null) {
            return socket.getSendBufferSize();
        } else {
            return super.getSendBufferSize();
        }
    }

    @Override
    public void setReceiveBufferSize(final int size) throws SocketException {
        if (socket != null) {
            socket.setReceiveBufferSize(size);
        } else {
            super.setReceiveBufferSize(size);
        }
    }

    @Override
    public int getReceiveBufferSize() throws SocketException {
        if (socket != null) {
            return socket.getReceiveBufferSize();
        } else {
            return super.getReceiveBufferSize();
        }
    }

    @Override
    public void setKeepAlive(final boolean on) throws SocketException {
        if (socket != null) {
            socket.setKeepAlive(on);
        } else {
            super.setKeepAlive(on);
        }
    }

    @Override
    public boolean getKeepAlive() throws SocketException {
        if (socket != null) {
            return socket.getKeepAlive();
        } else {
            return super.getKeepAlive();
        }
    }

    @Override
    public void setTrafficClass(final int tc) throws SocketException {
        if (socket != null) {
            socket.setTrafficClass(tc);
        } else {
            super.setTrafficClass(tc);
        }
    }

    @Override
    public int getTrafficClass() throws SocketException {
        if (socket != null) {
            return socket.getTrafficClass();
        } else {
            return super.getTrafficClass();
        }
    }

    @Override
    public void setReuseAddress(final boolean on) throws SocketException {
        if (socket != null) {
            socket.setReuseAddress(on);
        } else {
            super.setReuseAddress(on);
        }
    }

    @Override
    public boolean getReuseAddress() throws SocketException {
        if (socket != null) {
            return socket.getReuseAddress();
        } else {
            return super.getReuseAddress();
        }
    }

    @Override
    public void shutdownInput() throws IOException {
        if (socket != null) {
            socket.shutdownInput();
        } else {
            super.shutdownInput();
        }
    }

    @Override
    public void shutdownOutput() throws IOException {
        if (socket != null) {
            socket.shutdownOutput();
        } else {
            super.shutdownOutput();
        }
    }

    @Override
    public boolean isConnected() {
        if (socket != null) {
            return socket.isConnected();
        } else {
            return super.isConnected();
        }
    }

    @Override
    public boolean isBound() {
        if (socket != null) {
            return socket.isBound();
        } else {
            return super.isBound();
        }
    }

    @Override
    public boolean isClosed() {
        if (socket != null) {
            return socket.isClosed();
        } else {
            return super.isClosed();
        }
    }

    @Override
    public boolean isInputShutdown() {
        if (socket != null) {
            return socket.isInputShutdown();
        } else {
            return super.isInputShutdown();
        }
    }

    @Override
    public boolean isOutputShutdown() {
        if (socket != null) {
            return socket.isOutputShutdown();
        } else {
            return super.isOutputShutdown();
        }
    }

    @Override
    public void setPerformancePreferences(final int connectionTime, final int latency, final int bandwidth) {
        if (socket != null) {
            socket.setPerformancePreferences(connectionTime, latency, bandwidth);
        } else {
            super.setPerformancePreferences(connectionTime, latency, bandwidth);
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
}
