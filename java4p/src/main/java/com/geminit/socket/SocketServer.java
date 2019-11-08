package com.geminit.socket;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;

/**
 * Created by tyx on 11/8/19.
 */
public class SocketServer {
    public static void main(String[] args) {
        try {
            // 初始化服务端socket并且绑定9999端口
            ServerSocket serverSocket = new ServerSocket(0);
            //等待客户端的连接
            Socket socket = serverSocket.accept();
            //获取输入流
            BufferedReader bufferedReader =new BufferedReader(new InputStreamReader(socket.getInputStream(),"UTF-8"));

            String str;
            while ((str = bufferedReader.readLine())!=null) {
                System.out.println(str);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
