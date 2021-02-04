package com.nr.paho.mqtt3.examples;

import java.util.Random;

import org.eclipse.paho.client.mqttv3.IMqttMessageListener;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import com.newrelic.api.agent.NewRelic;
import com.newrelic.api.agent.Trace;
import com.newrelic.api.agent.TransactionNamePriority;

public class Application implements Runnable {

	private MqttClient client = null;
	private static long duration = 15*60*1000L;


	public static void main(String[] args) {
		Application app = new Application();

		app.run();
	}

	public void run() {
		try {
			init();
			subscribe();
			pauseSeconds(3);
			long start = System.currentTimeMillis();

			long running = System.currentTimeMillis() - start;
			int count = 1;
			while(running < duration) {
				publish(count);
				pauseSeconds(5);
				count++;
				running = System.currentTimeMillis() - start;
			}
			close();
		} catch (MqttException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void init() throws MqttException {
		client = new MqttClient("tcp://localhost:1883", "Doug", new MemoryPersistence());
		client.connect();
	}

	private static final Random random = new Random();
	private static final int MAX = 15;

	public void subscribe() throws MqttException {
		MessageListener listener = new MessageListener();
		client.subscribe(new String[] {Utils.TOPIC}, new int[] {2}, new IMqttMessageListener[] {listener});
	}

	@Trace(dispatcher=true)
	public void publish(int i) {
		NewRelic.getAgent().getTransaction().setTransactionName(TransactionNamePriority.CUSTOM_HIGH, true, "Custom", "Paho","MQTT5","publish");
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

	public void close() throws MqttException {
		client.disconnect();
	}

	public void pauseSeconds(int n) {
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
