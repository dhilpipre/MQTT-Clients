package com.nr.paho.mqtt3.examples;

import java.util.Random;

import org.eclipse.paho.client.mqttv3.IMqttActionListener;
import org.eclipse.paho.client.mqttv3.IMqttToken;
import org.eclipse.paho.client.mqttv3.MqttAsyncClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;

import com.newrelic.api.agent.NewRelic;
import com.newrelic.api.agent.Trace;
import com.newrelic.api.agent.TransactionNamePriority;

public class AsyncApplication implements Runnable {

	private MqttAsyncClient client = null;
	private static long duration = 15*60*1000L;


	public static void main(String[] args) {
		AsyncApplication app = new AsyncApplication();

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
		client = new MqttAsyncClient("tcp://localhost:1883", "Doug-Async", new MemoryPersistence());
	}

	private static final Random random = new Random();
	private static final int MAX = 15;

	public void subscribe() throws MqttException {
		client.connect().waitForCompletion();;

		MessageListener listener = new MessageListener();
		//IMqttToken result = client.subscribe(new MqttSubscription[] {subscription}, listener);
		IMqttActionListener callback = new IMqttActionListener() {
			
			@Override
			public void onSuccess(IMqttToken asyncActionToken) {
				System.out.println("Sucessfully subscribed to "+asyncActionToken.getTopics());
			}
			
			@Override
			public void onFailure(IMqttToken asyncActionToken, Throwable exception) {
				System.out.println("Failed to subscribe to "+asyncActionToken.getTopics());
				
			}
		};
		IMqttToken result = client.subscribe(Utils.ATOPIC, 2, null, callback, listener);
		result.waitForCompletion();
	}

	@Trace(dispatcher=true)
	public void publish(int i) {
		NewRelic.getAgent().getTransaction().setTransactionName(TransactionNamePriority.CUSTOM_HIGH, true, "Custom", "Paho","MQTT5","publish");
		System.out.println("publishing message "+i);
		String msg = "Hello, this is message #"+i;
		try {
			IMqttActionListener actionListener = new IMqttActionListener() {
				
				@Override
				public void onSuccess(IMqttToken asyncActionToken) {
					System.out.println("Successfully published message to "+Utils.printTopics(asyncActionToken.getTopics()));
					pauseRandomUnits();
				}
				
				@Override
				public void onFailure(IMqttToken asyncActionToken, Throwable exception) {
					System.out.println("Failed to publish message to "+Utils.printTopics(asyncActionToken.getTopics())+" due to error: "+exception.getMessage());
					pauseRandomUnits();
				}
			};
			client.publish(Utils.ATOPIC,msg.getBytes(),1,true,null,actionListener) ;
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
