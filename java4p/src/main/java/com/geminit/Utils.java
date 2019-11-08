package com.geminit;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by tyx on 11/8/19.
 */
public class Utils {
    public static void main(String[] args) {
        String s;
        StringBuilder sb = new StringBuilder();
        BufferedReader bufferedReader;
        try {
            Process getPysparkProcess = Runtime.getRuntime().exec("pip show pyspark");

            bufferedReader = new BufferedReader(new InputStreamReader(getPysparkProcess.getInputStream()));
            while((s=bufferedReader.readLine()) != null) {
                sb.append(s);
            }
            String pysparkInfo = sb.toString();
            Pattern r = Pattern.compile("Location: (.*?)Requires:");
            Matcher m = r.matcher(pysparkInfo);
            String pysparkPath = "";
            if (m.find( )) {
                pysparkPath = m.group(1);
            }
            String daemonPath = pysparkPath + "/pyspark/daemon.py";

            Process deamonProcess = Runtime.getRuntime().exec("python " + daemonPath);
            byte[] bytes = new byte[4];
            deamonProcess.getInputStream().read(bytes);
            int pythonPort = bytesToInt(bytes, 2);

            try {
                Socket socket = new Socket("127.0.0.1", pythonPort);
                BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(socket.getOutputStream()));
                bufferedWriter.write("Test");
                //刷新输入流
                bufferedWriter.flush();
                //关闭socket的输出流
                socket.shutdownOutput();
            } catch (Exception e) {
                e.printStackTrace();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

    }

    public static int bytesToInt(byte[] ary, int offset) {
        int value;
        value = (int) ((ary[offset]&0xFF)
                | ((ary[offset+1]<<8) & 0xFF00));
        return value;
    }
}
