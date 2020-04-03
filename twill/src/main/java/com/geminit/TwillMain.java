package com.geminit;

import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.twill.api.TwillController;
import org.apache.twill.api.TwillRunnerService;
import org.apache.twill.api.logging.PrinterLogHandler;
import org.apache.twill.yarn.YarnTwillRunnerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;

public class TwillMain {
    private static Logger LOG = LoggerFactory.getLogger(TwillMain.class);

    public static void main(String[] args) {
        if (args.length < 1) {
            args = new String[1];
            args[0] = "192.168.0.114:2181";
        }
        print("Zookeeper str: " + args[0]);
        YarnConfiguration yarnConfiguration = new YarnConfiguration();
        yarnConfiguration.set("yarn.resourcemanager.hostname", "htsp.htdata.com");
        yarnConfiguration.set("yarn.resourcemanager.address", "htsp.htdata.com:8050");
        yarnConfiguration.set("yarn.resourcemanager.scheduler.address", "htsp.htdata.com:8030");
        yarnConfiguration.set("yarn.resourcemanager.webapp.address", "htsp.htdata.com:8088");
        yarnConfiguration.set("yarn.resourcemanager.webapp.https.address", "htsp.htdata.com:8090");
        yarnConfiguration.set("yarn.resourcemanager.resource-tracker.address", "htsp.htdata.com:8025");
        yarnConfiguration.set("yarn.resourcemanager.admin.address", "htsp.htdata.com:8141");
        TwillRunnerService twillRunnerService = new YarnTwillRunnerService(yarnConfiguration, args[0]);
        twillRunnerService.start();
        print("twillRunnerService.start()");


        TwillController controller = twillRunnerService.prepare(new EchoServer())
                .addLogHandler(new PrinterLogHandler(new PrintWriter(System.out, true)))
                .start();
        print("twillRunnerService.start()");

        print("controller.getRunId(): " + controller.getRunId().getId());
    }

    private static void print(String content) {
        System.out.println("\n\n\n------------------------------------------------------------------------------------------\n\n");
        System.out.println(content);
        System.out.println("\n\n------------------------------------------------------------------------------------------\n\n\n");
    }
}
