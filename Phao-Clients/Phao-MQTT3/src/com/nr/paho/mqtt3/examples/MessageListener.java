package com.nr.paho.mqtt3.examples;

import java.util.Random;

import org.eclipse.paho.client.mqttv3.IMqttMessageListener;
import org.eclipse.paho.client.mqttv3.MqttMessage;

import com.newrelic.api.agent.Trace;

public class MessageListener implements IMqttMessageListener {
	
	private static final Random random = new Random();
	private static final int MAX = 15;


	@Override
	@Trace
	public void messageArrived(String topic, MqttMessage msg) throws Exception {
		System.out.println("Received message on topic "+topic);
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
