package com.htdata.plugin.spark;

import org.apache.spark.api.python.SpecialLengths;

import java.io.BufferedOutputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.net.Socket;
import java.util.Iterator;

/**
 * Created by tyx on 11/12/19.
 */
public class WriteThread extends Thread {
    private String fileUri;
    private String localizePath;
    private Socket worker;
    private Iterator<byte[]> iterator;

    private static Object key = new Object();

    public WriteThread(String fileUri, String localizePath, Socket worker, Iterator<byte[]> iterator) {
        this.fileUri = fileUri;
        this.localizePath = localizePath;
        this.worker = worker;
        this.iterator = iterator;
    }

    @Override
    public void run() {
        try {
            BufferedOutputStream bufferedStream = new BufferedOutputStream(worker.getOutputStream(), 65536);
            DataOutputStream dataOut = new DataOutputStream(bufferedStream);

            synchronized (WriteThread.key) {
                if (!new File(localizePath).exists()) {
                    Util.downloadAndUnzip(fileUri, localizePath);
                }
            }

            Util.writeUTF(localizePath, dataOut);
            while (iterator.hasNext()) {
                byte[] arr = new byte[0];
                try {
                    arr = iterator.next();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                dataOut.writeInt(arr.length);
                dataOut.write(arr);
            }
            dataOut.writeInt(SpecialLengths.END_OF_DATA_SECTION());

            dataOut.flush();
        } catch (IOException e) {
            e.printStackTrace();
            Util.deleteDir(localizePath);
        }
    }
}
