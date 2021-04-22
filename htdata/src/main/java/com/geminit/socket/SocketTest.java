package com.geminit.socket;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;

public class SocketTest {
    public static void main(String[] args) {
        Socket socket;
        DataOutputStream dataOut;
        DataInputStream dataIn;
        try {
            socket = new Socket("192.168.0.112", 11111);
            dataOut = new DataOutputStream(new BufferedOutputStream(socket.getOutputStream(), 65536));
            dataIn = new DataInputStream(new BufferedInputStream(socket.getInputStream(), 65536));
        } catch (IOException e) {
            return;
        }

        try {
            if (dataIn.readInt() == 1) {
                // forked
                int length = dataIn.readInt();
                if (length >= 0) {
                    // experiment data
                    byte[] bytes = new byte[length];
                    dataIn.read(bytes);
                    String experiment = new String(bytes);
                } else {
                    // exception occurred in worker
                    // length == -1
                }
            } else {
                // fork failed
            }

//            byte[] resBytes = new byte[dataIn.readInt()];
//            dataIn.read(resBytes);
//            String error = new String(resBytes, "utf-8");

            dataOut.close();
            dataIn.close();
            socket.close();
        } catch (IOException e) {
            System.out.println();
        }
    }
}
