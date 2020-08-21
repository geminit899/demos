package com.geminit;

import java.net.Socket;

public class SocketTest {
    private static int daemonPort = 0;

    public static void main(String[] args) throws Exception {
        Socket socket = new Socket("127.0.0.1", daemonPort);
    }
}
