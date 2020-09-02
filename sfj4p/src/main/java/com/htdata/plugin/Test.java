package com.htdata.plugin;

import java.net.*;
import java.io.*;
import java.nio.charset.*;

public class Test {
    public static void main(String[] args) throws Exception {
        Socket socket = new Socket("127.0.0.1", 52251);

        WriteThread writeThread = new WriteThread(socket);
        writeThread.start();

        BufferedInputStream inputStream = new BufferedInputStream(socket.getInputStream(), 65536);
        DataInputStream dataIn = new DataInputStream(inputStream);

        while (true) {
            int length = dataIn.readInt();
            if (length == -1) {
                break;
            }
            byte[] resBytes = new byte[length];
            dataIn.read(resBytes);
            String res = new String(resBytes, "utf-8");
            System.out.println(res);
        }
        dataIn.close();
        socket.close();
    }
}

class WriteThread extends Thread {
    private Socket worker;
    public WriteThread(Socket worker) {
        this.worker = worker;
    }

    @Override
    public void run() {
        try {
            DataOutputStream dataOut = new DataOutputStream(new BufferedOutputStream(this.worker.getOutputStream(), 65536));

            writeUTF("测试一下test 888", dataOut);

            writeUTF("好的1", dataOut);
            writeUTF("好的2", dataOut);
            writeUTF("好的3", dataOut);
            dataOut.writeInt(-1);

            dataOut.flush();
        } catch (Exception e) {

        }
    }

    public void writeUTF(String str, DataOutputStream dataOut) throws IOException {
        byte[] bytes = str.getBytes(StandardCharsets.UTF_8);
        dataOut.writeInt(bytes.length);
        dataOut.write(bytes);
    }
}