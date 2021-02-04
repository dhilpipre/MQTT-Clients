package com.nr.paho.mqtt3.examples;

import java.util.Random;

import org.eclipse.paho.client.mqttv3.IMqttMessageListener;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

public class SyncSubscriberThread extends Thread {

	private MqttClient client = null;

	private static final Random random = new Random();
	private static final int MAX = 15;

	public SyncSubscriberThread() throws MqttException {
		client = new MqttClient("tcp://localhost:1883", "Doug-Sub", new MemoryPersistence());
	}

	@Override
	public void run() {

			try {
				client.connect();
				IMqttMessageListener listener = (tpic, msg) -> {
					System.out.println(msg.getId() + " -> " + new String(msg.getPayload()));
					pauseRandomUnits();
				};
				
				client.subscribe(new String[] {Utils.TOPIC}, new int[] {2}, new IMqttMessageListener[] {listener});
				System.out.println("Have subscribed to topic");
				client.disconnect();
			} catch (MqttException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
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
