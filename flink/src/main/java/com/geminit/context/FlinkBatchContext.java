package com.geminit.context;

import org.apache.flink.api.java.RemoteEnvironment;
import org.apache.flink.configuration.Configuration;

import java.net.URL;

public class FlinkBatchContext extends RemoteEnvironment implements FlinkContext {

	public FlinkBatchContext(String host, int port, String... jarFiles) {
		this(host, port, (Configuration)null, jarFiles, (URL[])null);
	}

	public FlinkBatchContext(String host, int port, Configuration clientConfig, String[] jarFiles) {
		this(host, port, clientConfig, jarFiles, (URL[])null);
	}

	public FlinkBatchContext(String host, int port, Configuration clientConfig, String[] jarFiles, URL[] globalClasspaths) {
		super(host, port, clientConfig, jarFiles, globalClasspaths);
	}
}
