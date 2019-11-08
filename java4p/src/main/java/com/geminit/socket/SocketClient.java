package com.geminit.socket;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.net.Socket;

public class SocketClient {
    public static void main(String[] args) {
        try {
            Socket socket = new Socket("127.0.0.1", 0);
            BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
            bufferedWriter.write("Test");
            //刷新输入流
            bufferedWriter.flush();
            //关闭socket的输出流
            socket.shutdownOutput();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
