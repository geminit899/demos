package com.geminit.context;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.RemoteStreamEnvironment;

import java.net.URL;

public class FlinkStreamContext extends RemoteStreamEnvironment implements FlinkContext {

	public FlinkStreamContext(String host, int port, String... jarFiles) {
		this(host, port, (Configuration)null, jarFiles);
	}

	public FlinkStreamContext(String host, int port, Configuration clientConfiguration, String... jarFiles) {
		this(host, port, clientConfiguration, jarFiles, (URL[])null);
	}

	public FlinkStreamContext(String host, int port, Configuration clientConfiguration, String[] jarFiles, URL[] globalClasspaths) {
		super(host, port, clientConfiguration, jarFiles, globalClasspaths);
	}
}
