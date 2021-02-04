package com.nr.paho.mqtt3.examples;

import java.util.Random;

import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttSecurityException;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import com.newrelic.api.agent.Trace;

public class SyncPublisherThread extends Thread {
	
	private MqttClient client = null;

	private static final Random random = new Random();
	private static final int MAX = 15;
	
	public SyncPublisherThread() throws MqttException {
		client = new MqttClient("tcp://localhost:1883", "Doug-Pub", new MemoryPersistence());

	}

	@Override
	public void run() {
		try {
			int count = 1;
			client.connect();
			
			while(count < 5) {
				publish(count);
				count++;
				pauseSeconds(10);
			}
		} catch (MqttSecurityException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (MqttException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Trace(dispatcher=true)
	public void publish(int i) {
		System.out.println("publishing message "+i);
		String msg = "Hello, this is message #"+i;
		try {
			client.publish(Utils.TOPIC,msg.getBytes(),1,true) ;
		} catch (MqttException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		pauseRandomUnits();
	}
	
	private void pauseSeconds(int n) {
		pause(n*1000L);
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
