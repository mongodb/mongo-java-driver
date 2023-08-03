package com.mongodb.internal.connection;

import com.mongodb.assertions.Assertions;
import com.mongodb.lang.Nullable;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.net.SocketException;
import java.net.SocketTimeoutException;


public class SocksSocket extends Socket {

    private static final byte SOCKS_VERSION = 5;
    private static final byte RESERVED = 0x00;
    private static final byte AUTHENTICATION_SUCCEEDED_STATUS = 0x00;
    static final int REQUEST_OK = 0;
    static final int GENERAL_FAILURE = 1;
    static final int NOT_ALLOWED = 2;
    static final int NET_UNREACHABLE = 3;
    static final int HOST_UNREACHABLE = 4;
    static final int CONN_REFUSED = 5;
    static final int TTL_EXPIRED = 6;
    static final int CMD_NOT_SUPPORTED = 7;
    static final int ADDR_TYPE_NOT_SUP = 8;
    static final int IPV4 = 1;
    static final int DOMAIN_NAME = 3;
    static final int IPV6 = 4;
    private InetSocketAddress proxyAddress;
    private InetSocketAddress remoteAddress;
    private String proxyUsername;
    private String proxyPassword;
    private final SocksAuthenticationMethod[] authenticationMethods;
    private final Socket socket;
    private InputStream inputStream;
    private OutputStream outputStream;


    public SocksSocket(@Nullable final Socket socket, final InetSocketAddress proxyAddress, final String proxyUsername,
                       final String proxyPassword) {
        this.authenticationMethods = new SocksAuthenticationMethod[]{
                SocksAuthenticationMethod.NO_AUTH,
                SocksAuthenticationMethod.USERNAME_PASSWORD};
        this.proxyUsername = proxyUsername;
        this.proxyPassword = proxyPassword;
        this.proxyAddress = proxyAddress;
        this.socket = socket;
    }

    public SocksSocket(@Nullable final Socket socket, final InetSocketAddress proxyAddress) {
        this.authenticationMethods = new SocksAuthenticationMethod[]{SocksAuthenticationMethod.NO_AUTH};
        this.proxyAddress = proxyAddress;
        this.socket = socket;
    }

    private static int remainingMillis(final long deadlineMillis) throws IOException {
        if (deadlineMillis == 0L) {
            return 0;
        }

        final long remaining = deadlineMillis - System.currentTimeMillis();
        if (remaining > 0) {
            return (int) remaining;
        }

        throw new SocketTimeoutException("Socket connection timed out");
    }


    private byte[] readSocksReply(final InputStream in, final int length, final long deadlineMillis) throws IOException {
        byte[] data = new byte[length];
        int received = 0;
        int originalTimeout = getSoTimeout();
        try {
            while (received < length) {
                int count;
                int remaining = remainingMillis(deadlineMillis);
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
    public void connect(final SocketAddress endpoint) throws IOException {
        connect(endpoint, 0);
    }

    @Override
    public void connect(final SocketAddress endpoint, final int timeout) throws IOException {
        try {
            remoteAddress = ((InetSocketAddress) endpoint);
            Assertions.assertTrue(remoteAddress.isUnresolved());

            final long deadlineMillis = getDeadlineMillis(timeout);
            if (socket != null && !socket.isConnected()) {
                socket.connect(proxyAddress, timeout); //TCP connect
                inputStream = socket.getInputStream();
                outputStream = socket.getOutputStream();
            } else {
                super.connect(proxyAddress, timeout);
                inputStream = getInputStream();
                outputStream = getOutputStream();
            }
            SocksAuthenticationMethod authenticationMethod = doHandshake(deadlineMillis);
            authenticate(authenticationMethod, deadlineMillis);
            sendConnect(deadlineMillis);
        } catch (SocketException socketException) {
            close();
        }
    }

    private void sendConnect(final long deadlineMillis) throws IOException {
        String host = remoteAddress.getHostName();
        int port = remoteAddress.getPort();

        final int lengthOfHost = host.getBytes().length;
        final byte[] bufferSent = new byte[7 + lengthOfHost];

        bufferSent[0] = SOCKS_VERSION;
        bufferSent[1] = (byte) SocksCommand.CONNECT.getCommandNumber();
        bufferSent[2] = RESERVED;
        bufferSent[3] = DOMAIN_NAME;  //TODO chose appropriate type. Currently it is DOMAIN NAME ;
        bufferSent[4] = (byte) lengthOfHost;
        byte[] bytesOfHost = host.getBytes();
        System.arraycopy(bytesOfHost, 0, bufferSent, 5, lengthOfHost);
        bufferSent[5 + host.length()] = (byte) ((port & 0xff00) >> 8);
        bufferSent[6 + host.length()] = (byte) (port & 0xff);

        outputStream.write(bufferSent);
        outputStream.flush();

        checkServerReply(deadlineMillis);
    }

    private void checkServerReply(final long deadlineMillis) throws IOException {
        byte[] data = readSocksReply(inputStream, 4, deadlineMillis);
        byte status = data[1];
        switch (status) {
            case REQUEST_OK:
                int portSize = 2;
                switch (data[3]) {
                    case IPV4:
                        readSocksReply(inputStream, 4 + portSize, deadlineMillis);
                        break;
                    case DOMAIN_NAME:
                        byte hostNameLength = readSocksReply(inputStream, 1, deadlineMillis)[0];
                        readSocksReply(inputStream, hostNameLength + portSize, deadlineMillis);
                        break;
                    case IPV6:
                        readSocksReply(inputStream, 16 + portSize, deadlineMillis);
                        break;
                    default:
                        throw new ConnectException("Reply from SOCKS proxy server contains wrong code");
                }
                break;
            case GENERAL_FAILURE:
                throw new ConnectException("SOCKS: Proxy server general failure");
            case NOT_ALLOWED:
                throw new ConnectException("SOCKS: Connection not allowed by ruleset");
            case NET_UNREACHABLE:
                throw new ConnectException("SOCKS: Network unreachable");
            case HOST_UNREACHABLE:
                throw new ConnectException("SOCKS: Host unreachable");
            case CONN_REFUSED:
                throw new ConnectException("SOCKS: Connection refused");
            case TTL_EXPIRED:
                throw new ConnectException("SOCKS: TTL expired");
            case CMD_NOT_SUPPORTED:
                throw new ConnectException("SOCKS: Command not supported");
            case ADDR_TYPE_NOT_SUP:
                throw new ConnectException("SOCKS: Address type not supported");
            default:
                throw new ConnectException("SOCKS: Unknown status");
        }
    }

    private void authenticate(final SocksAuthenticationMethod authenticationMethod, final long deadlineMillis) throws IOException {
        if (authenticationMethod == SocksAuthenticationMethod.USERNAME_PASSWORD) {

            final int usernameLength = proxyUsername.getBytes().length;
            final int passwordLength = proxyPassword.getBytes().length;
            final byte[] bytesOfUsername = proxyUsername.getBytes();
            final byte[] bytesOfPassword = proxyPassword.getBytes();
            final byte[] command = new byte[3 + usernameLength + passwordLength];

            command[0] = 0x01;
            command[1] = (byte) proxyUsername.getBytes().length;
            System.arraycopy(bytesOfUsername, 0, command, 2, usernameLength);
            command[2 + usernameLength] = (byte) passwordLength;
            System.arraycopy(bytesOfPassword, 0, command, 3 + usernameLength,
                    passwordLength);
            outputStream.write(command);
            outputStream.flush();

            byte[] authenticationResult = readSocksReply(inputStream, 2, deadlineMillis);

            if (authenticationResult[1] != AUTHENTICATION_SUCCEEDED_STATUS) {
                  /* RFC 1929 specifies that the connection MUST be closed if
                   authentication fails */
                close(); //TODO close in and out streams. Maybe override close and close them there
                throw new ConnectException("Username or password is incorrect");
            }
        }
    }

    private SocksAuthenticationMethod doHandshake(final long deadlineMillis) throws IOException {
        int methodsCount = authenticationMethods.length;

        byte[] bufferSent = new byte[2 + methodsCount];
        bufferSent[0] = SOCKS_VERSION;
        bufferSent[1] = (byte) methodsCount;
        for (int i = 0; i < methodsCount; i++) {
            bufferSent[2 + i] = (byte) authenticationMethods[i].getMethodNumber();
        }
        outputStream.write(bufferSent);
        outputStream.flush();

        byte[] handshakeReply = readSocksReply(inputStream, 2, deadlineMillis);

        if (handshakeReply[0] != SOCKS_VERSION) {
            close();
            throw new ConnectException("Remote server don't support SOCKS5");
        }
        if (handshakeReply[1] == (byte) 0xFF) {
            close();
            throw new ConnectException("None of the authentication methods listed are acceptable");
        }

        if (handshakeReply[1] == SocksAuthenticationMethod.NO_AUTH.getMethodNumber()) {
            return SocksAuthenticationMethod.NO_AUTH;
        }

        return SocksAuthenticationMethod.USERNAME_PASSWORD;
    }

    private static long getDeadlineMillis(final int timeout) {
        final long deadlineMillis;
        if (timeout == 0) {
            deadlineMillis = 0L;
        } else {
            long finish = System.currentTimeMillis() + timeout;
            deadlineMillis = finish < 0 ? Long.MAX_VALUE : finish;
        }
        return deadlineMillis;
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
        return inputStream;
    }

    @Override
    public OutputStream getOutputStream() {
        return outputStream;
    }

    private enum SocksCommand {

        CONNECT(0x01);

        private int value;

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

        private int methodNumber;

        SocksAuthenticationMethod(final int methodNumber) {
            this.methodNumber = methodNumber;
        }

        public int getMethodNumber() {
            return methodNumber;
        }

    }
}
