package com.geminit.context;

public class FlinkContextTest {
	public static void main(String[] args) {
		String mode = "BATCH";

		FlinkContext context = null;

		switch (mode){
			case "BATCH":
				context = new FlinkBatchContext("192.168.0.119", 9093);
				break;
			case "STREAM":
				context = new FlinkStreamContext("192.168.0.119", 9093);
		}

		String sss = context.getClass().getName();
		System.out.println(sss);
		System.out.println(context.toString());
		FlinkBatchContext test = (FlinkBatchContext)context;
		System.out.println(test.toString());
	}
}
