package com.htdata.plugin.spark;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.net.URL;

/**
 *  A tool class for JavaRDD to compute in Python.
 */
public class PythonFactory {

    private String daemonPath;
    private String shellPath;

    private Process daemon = null;
    private int daemonPort = 0;

    public PythonFactory() throws IOException {
        ClassLoader classLoader = PythonFactory.class.getClassLoader();
        URL daemonURL = classLoader.getResource("spark.py");
        URL shellURL = classLoader.getResource("spark.sh");
        String daemonFilePath = daemonURL.getPath();
        String shellFilePath = shellURL.getPath();
        if (daemonFilePath.startsWith("file:/")) {
            daemonFilePath = daemonFilePath.substring(5);
        }
        if (shellFilePath.startsWith("file:/")) {
            shellFilePath = shellFilePath.substring(5);
        }
        File daemonFile = new File(daemonFilePath);
        File shellFile = new File(shellFilePath);
        if (daemonFile.exists()) {
            this.daemonPath = daemonFile.getPath();
        } else {
            File jarDir = new File(daemonFilePath).getParentFile().getParentFile();
            daemonFile = new File(jarDir, "spark.py");
            if (!daemonFile.exists()) {
                InputStream inputStream = classLoader.getResourceAsStream("spark.py");
                int fileSize = inputStream.available();
                byte[] inBytes = new byte[fileSize];
                inputStream.read(inBytes);
                inputStream.close();
                FileOutputStream outputStream = new FileOutputStream(daemonFile);
                outputStream.write(inBytes);
                outputStream.flush();
                this.daemonPath = daemonFile.getPath();
            }
        }
        if (shellFile.exists()) {
            this.shellPath = shellFile.getPath();
        } else {
            File jarDir = new File(shellFilePath).getParentFile().getParentFile();
            shellFile = new File(jarDir, "spark.sh");
            if (!shellFile.exists()) {
                InputStream inputStream = classLoader.getResourceAsStream("spark.sh");
                int fileSize = inputStream.available();
                byte[] inBytes = new byte[fileSize];
                inputStream.read(inBytes);
                inputStream.close();
                FileOutputStream outputStream = new FileOutputStream(shellFile);
                outputStream.write(inBytes);
                outputStream.flush();
                this.shellPath = shellFile.getPath();
            }
        }
    }

    public Socket create() throws IOException {
        // Start the daemon if it hasn't been started
        startDaemon();
        try {
            Thread.sleep(3000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return createSocket();
    }

    private Socket createSocket() throws IOException {
        // Attempt to connect, restart and retry once if it fails
        Socket socket;
        try {
            socket = new Socket("127.0.0.1", this.daemonPort);
        } catch (IOException e) {
            System.out.println("Failed to open socket to Python daemon:" + e);
            System.out.println("Assuming that daemon unexpectedly quit, attempting to restart");
            stopDaemon();
            startDaemon();
            socket = new Socket("127.0.0.1", this.daemonPort);
        }
        return socket;
    }

    private void startDaemon() throws IOException {
        if (this.daemon != null) {
            return;
        }

        try {
            String shell = "if command -v conda &> /dev/null\n" +
                    "then\n" +
                    "  source deactivate\n" +
                    "  conda activate htsc\n" +
                    "  python " + this.daemonPath + "\n" +
                    "else\n" +
                    "  python3 " + this.daemonPath + "\n" +
                    "fi";
            FileOutputStream outputStream = new FileOutputStream(new File(this.shellPath));
            outputStream.write(shell.getBytes());
            outputStream.flush();
            outputStream.close();

            String bashPath = new ProcessBuilder("").environment().get("SHELL");
            if (bashPath == null) {
                bashPath = "/bin/bash";
            }
            this.daemon = Runtime.getRuntime().exec(bashPath + " " + this.shellPath);

            DataInputStream in = new DataInputStream(this.daemon.getInputStream());
            this.daemonPort = in.readInt();
        } catch (IOException e) {
            stopDaemon();
            throw e;
        }
    }

    private void stopDaemon() {
        //Request shutdown of existing daemon by sending SIGTERM
        if (this.daemon != null) {
            this.daemon.destroy();
        }

        this.daemon = null;
        this.daemonPort = 0;
    }

    public void stop() {
        stopDaemon();
    }
}
