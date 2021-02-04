package com.nr.paho.mqtt5.examples;

import java.util.Random;

import org.eclipse.paho.mqttv5.client.IMqttToken;
import org.eclipse.paho.mqttv5.client.MqttActionListener;
import org.eclipse.paho.mqttv5.client.MqttAsyncClient;
import org.eclipse.paho.mqttv5.client.persist.MemoryPersistence;
import org.eclipse.paho.mqttv5.common.MqttException;
import org.eclipse.paho.mqttv5.common.MqttSubscription;
import org.eclipse.paho.mqttv5.common.packet.MqttProperties;

import com.newrelic.api.agent.NewRelic;
import com.newrelic.api.agent.Trace;
import com.newrelic.api.agent.TransactionNamePriority;

public class AsyncApplication implements Runnable {

	private MqttAsyncClient client = null;
	private static long duration = 15*60*1000L;
	private String hivehost = "localhost";
	private String clientName = "Tester-Async";

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
		String url = "tcp://"+hivehost+":1883";
		client = new MqttAsyncClient(url, clientName, new MemoryPersistence());
	}

	private static final Random random = new Random();
	private static final int MAX = 15;

	public void subscribe() throws MqttException {
		client.connect().waitForCompletion();;

		MqttSubscription subscription = new MqttSubscription(Utils.ATOPIC, 1);
		MessageListener listener = new MessageListener();
		//IMqttToken result = client.subscribe(new MqttSubscription[] {subscription}, listener);
		MqttActionListener callback = new MqttActionListener() {
			
			@Override
			public void onSuccess(IMqttToken asyncActionToken) {
				System.out.println("Sucessfully subscribed to "+asyncActionToken.getTopics());
			}
			
			@Override
			public void onFailure(IMqttToken asyncActionToken, Throwable exception) {
				System.out.println("Failed to subscribe to "+asyncActionToken.getTopics());
				
			}
		};
		MqttProperties subscriptionProperties = new MqttProperties();
		IMqttToken result = client.subscribe(new MqttSubscription[] {subscription}, null, callback, new MessageListener[] {listener}, subscriptionProperties);
		result.waitForCompletion();
	}

	@Trace(dispatcher=true)
	public void publish(int i) {
		NewRelic.getAgent().getTransaction().setTransactionName(TransactionNamePriority.CUSTOM_HIGH, true, "Custom", "Paho","MQTT5","publish");
		System.out.println("publishing message "+i);
		String msg = "Hello, this is message #"+i;
		try {
			MqttActionListener actionListener = new MqttActionListener() {
				
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
