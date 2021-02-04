package com.nr.paho.mqtt3.examples;

public class Utils {

	public static final String TOPIC = "test/paho-mqtt5";

	public static final String ATOPIC = "test/paho-mqtt5-async";
	
	public static String printTopics(String[] topics) {
		StringBuffer sb = new StringBuffer();
		
		for(int i=0;i<topics.length;i++) {
			sb.append(topics[i]);
			if(i < topics.length -1) {
				sb.append(", ");
			}
		}
		return sb.toString();
	}
}
