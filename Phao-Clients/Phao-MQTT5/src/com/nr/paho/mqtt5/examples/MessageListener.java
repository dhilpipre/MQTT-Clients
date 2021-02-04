package com.nr.paho.mqtt5.examples;

import java.util.List;
import java.util.Random;

import org.eclipse.paho.mqttv5.client.IMqttMessageListener;
import org.eclipse.paho.mqttv5.common.MqttMessage;
import org.eclipse.paho.mqttv5.common.packet.UserProperty;

import com.newrelic.api.agent.Trace;

public class MessageListener implements IMqttMessageListener {
	
	private static final Random random = new Random();
	private static final int MAX = 15;
	
	private int messageCount = 0;
	private long startTime;


	public int getMessageCount() {
		return messageCount;
	}

	public long getCurrentDuration() {
		return System.currentTimeMillis() - startTime;
	}

	@Override
	@Trace
	public void messageArrived(String topic, MqttMessage msg) throws Exception {
		messageCount++;
		if(messageCount == 1) {
			startTime = System.currentTimeMillis();
		}
		System.out.println("Received message on topic "+topic);
		List<UserProperty> userProps = msg.getProperties().getUserProperties();
		StringBuffer sb = new StringBuffer();
		int size = userProps.size();
		for(int i=0;i<size;i++) {
			sb.append(userProps.get(i).toString());
			if(i < size-1) {
				sb.append(',');
			}
		}
		System.out.println("Has properties: "+sb.toString());
		System.out.println(msg.getId() + " -> " + new String(msg.getPayload()));
		pauseRandomUnits();
	}

	
	private void pauseRandomUnits() {
		int n = random.nextInt(MAX);
		pause(n*100L);
	}

	private void pause(long ms) {
		if(ms > 0) {
			try {
				Thread.sleep(ms);
			} catch(InterruptedException e) {
				e.printStackTrace();
			}
		}

	}

}
