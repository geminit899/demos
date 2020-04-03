package com.geminit;

import org.apache.twill.api.AbstractTwillRunnable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EchoServer extends AbstractTwillRunnable {
    private static Logger LOG = LoggerFactory.getLogger(EchoServer.class);

    @Override
    public void run() {
        while (true) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            LOG.info("Twill application is running.");
        }
    }
}
