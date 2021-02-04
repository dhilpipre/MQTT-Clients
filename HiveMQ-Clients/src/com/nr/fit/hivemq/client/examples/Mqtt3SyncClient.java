package com.nr.fit.hivemq.client.examples;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.hivemq.client.mqtt.MqttGlobalPublishFilter;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt3.Mqtt3BlockingClient;
import com.hivemq.client.mqtt.mqtt3.Mqtt3BlockingClient.Mqtt3Publishes;
import com.hivemq.client.mqtt.mqtt3.Mqtt3Client;
import com.hivemq.client.mqtt.mqtt3.message.publish.Mqtt3Publish;
import com.newrelic.api.agent.Trace;

public class Mqtt3SyncClient {

	static int max = 300;
	
	private String hivehost = "localhost";

	public static void main(String[] args) {
		Mqtt3SyncClient clientApp = new Mqtt3SyncClient(args);
		clientApp.init();

	}
	
	public Mqtt3SyncClient(String[] args) {
		if(args.length > 0) {
			for(int i=0;i<args.length;i++) {
				String arg = args[i];
				if(arg.startsWith("-host")) {
					int index = arg.indexOf('=');
					if(index > -1) {
						String host = arg.substring(index+1);
						hivehost = host;
					}
				}
				if(arg.startsWith("-messages")) {
					int index = arg.indexOf('=');
					if(index > -1) {
						String maxStr = arg.substring(index+1);
						if(!maxStr.isEmpty()) {
							try {
								max = Integer.parseInt(maxStr);
							} catch(NumberFormatException e) {
								System.out.println("Unable to parse number of messages from "+maxStr);
							}
						}
					}
				}
			}
		}
	}

	public void init() {
		SubscriberThread subThread = new SubscriberThread();
		
		subThread.start();
		
		PublisherThread pubThread = new PublisherThread();
		pubThread.start();
	}
	
	@Trace(dispatcher=true)
	public void subscribe(Mqtt3BlockingClient client, int c) {
		System.out.println("Call to subscribe, count = "+c);
		client.connect();
		client.subscribeWith().topicFilter("test/nrdoug").qos(MqttQos.EXACTLY_ONCE).send();
		final Mqtt3Publishes publishes = client.publishes(MqttGlobalPublishFilter.ALL);
				
		try 
	    {

			Optional<Mqtt3Publish> option = publishes.receive(10, TimeUnit.SECONDS);
			processIncoming(option,"first");
			option = publishes.receive(100,TimeUnit.MILLISECONDS);
			processIncoming(option,"second");

		} catch (InterruptedException e) {
			e.printStackTrace();
		} finally {
			client.disconnect();
		}
	}
	
	@Trace(dispatcher=true)
	public void publish(Mqtt3BlockingClient client, String payload) {
		System.out.println("Call to publish("+payload+")");
		client.connect();
		
		client.publishWith().topic("test/nrdoug").qos(MqttQos.AT_LEAST_ONCE).payload(payload.getBytes()).send();
		client.disconnect();
		pause(600L);
	}

	private void pause(long ms) {
		if(ms > 0) {
			try {
				Thread.sleep(ms);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}


	@Trace
	public void processIncoming(Optional<Mqtt3Publish> option,String messageNumber) {
		if(option.isPresent()) {
			Mqtt3Publish pub = option.get();
			System.out.println(messageNumber+" message received: "+new String(pub.getPayloadAsBytes()));
		} else {
			System.out.println("no "+messageNumber+" message received");
		}
		
	}
	
	private class PublisherThread extends Thread {
		Mqtt3BlockingClient client = Mqtt3Client.builder().identifier(UUID.randomUUID().toString())
				.serverHost(hivehost).build().toBlocking();

		int count = 0;

		public PublisherThread() {
			super("PublisherThread");
		}

		@Override
		public void run() {
			while(count < max) {
				publish(client,Integer.toString(count));
				pause(5000L);
				count++;
			}
		}

		private void pause(long ms) {
			if(ms > 0) {
				try {
					sleep(ms);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}

	}
	
	private class SubscriberThread extends Thread {
		
		Mqtt3BlockingClient client = Mqtt3Client.builder().identifier(UUID.randomUUID().toString())
				.serverHost(hivehost).build().toBlocking();

		int count = 0;

		public SubscriberThread() {
			super("SubscriberThread");
		}

		@Override
		public void run() {
			while(count < max) {

				subscribe(client, count);
				pause(4000L);
				count++;
			}
		}

		private void pause(long ms) {
			if(ms > 0) {
				try {
					sleep(ms);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
		}

	}

}
