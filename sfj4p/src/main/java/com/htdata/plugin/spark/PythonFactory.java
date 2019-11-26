package com.htdata.plugin.spark;

import org.apache.spark.api.python.PythonUtils;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.Socket;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 *  A tool class for JavaRDD to compute in Python.
 */
public class PythonFactory {

    private String pythonExec = "python";
    private String daemonPath;

    private Process daemon = null;
    private int daemonPort = 0;

    private String pythonPath;

    public PythonFactory() throws IOException {
        ClassLoader classLoader = PythonFactory.class.getClassLoader();
        URL daemonURL = classLoader.getResource("spark.py");
        String daemonFilePath = daemonURL.getPath();
        if (daemonFilePath.startsWith("file:/")) {
            daemonFilePath = daemonFilePath.substring(5);
        }
        File daemonFile = new File(daemonFilePath);
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

        List<String> pythonPaths = new ArrayList<>();
        pythonPaths.add(PythonUtils.sparkPythonPath());
        pythonPaths.add(System.getenv().getOrDefault("PYTHONPATH", ""));
        this.pythonPath = PythonUtils.mergePythonPaths(PythonUtils.toSeq(pythonPaths));
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
            ProcessBuilder pb = new ProcessBuilder(Arrays.asList(this.pythonExec, this.daemonPath));
            Map<String, String> workerEnv = pb.environment();
            workerEnv.put("PYTHONPATH",this.pythonPath);
            //This is equivalent to setting the -u flag; we use it because ipython doesn't support -u:
            workerEnv.put("PYTONUNBUFFERED", "YES");
            this.daemon = pb.start();

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
