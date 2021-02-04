package com.nr.fit.hivemq.client.examples;

import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.hivemq.client.internal.mqtt.datatypes.MqttUserPropertiesImpl;
import com.hivemq.client.internal.mqtt.datatypes.MqttUserPropertyImpl;
import com.hivemq.client.internal.util.collections.ImmutableList;
import com.hivemq.client.mqtt.MqttClient;
import com.hivemq.client.mqtt.MqttGlobalPublishFilter;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient;
import com.hivemq.client.mqtt.mqtt5.Mqtt5BlockingClient.Mqtt5Publishes;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import com.newrelic.api.agent.Trace;

public class SyncClient {

	static int max = 300;
	private String hivehost = "localhost";

	public static void main(String[] args) {
		SyncClient sClient = new SyncClient(args);
		sClient.initialize();
	}

	public SyncClient(String[] args) {
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

	public void initialize() {
		PublisherThread pThread = new PublisherThread();
		SubscriberThread sThread = new SubscriberThread();

		pThread.start();
		sThread.start();
	}
	
	@Trace
	public void processIncoming(Optional<Mqtt5Publish> option,String messageNumber) {
		if(option.isPresent()) {
			Mqtt5Publish pub = option.get();
			System.out.println("Message from topic "+pub.getTopic().toString());
			System.out.println(messageNumber+" message received: "+new String(pub.getPayloadAsBytes()));
			System.out.println("message user properties: "+pub.getUserProperties());
		} else {
			System.out.println("no "+messageNumber+" message received");
		}
		
	}

	@Trace(dispatcher=true)
	public void subscribe(Mqtt5BlockingClient client, int c) {
		System.out.println("Call to subscribe, count = "+c);
		client.connect();
		client.subscribeWith().topicFilter("test/nrdoug").qos(MqttQos.EXACTLY_ONCE).send();
		final Mqtt5Publishes publishes = client.publishes(MqttGlobalPublishFilter.ALL);
				
		try 
	    {

			Optional<Mqtt5Publish> option = publishes.receive(10, TimeUnit.SECONDS);
			processIncoming(option,"first");
			option = publishes.receive(100,TimeUnit.MILLISECONDS);
			processIncoming(option,"second");

		} catch (InterruptedException e) {
			e.printStackTrace();
		} finally {
			client.disconnect();
		}
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

	@Trace(dispatcher=true)
	public void publish(Mqtt5BlockingClient client, String payload) {
		System.out.println("Call to publish("+payload+")");
		client.connect();
		Integer i = null;
		
		try {
			i = Integer.parseInt(payload);
		} catch (NumberFormatException e) {
			i = new Integer(-1);
		}
		MqttUserPropertyImpl property = MqttUserPropertyImpl.of("MessageCount", Integer.toString(i));
		
		ImmutableList<MqttUserPropertyImpl> propList = ImmutableList.of(property);
		MqttUserPropertiesImpl props = MqttUserPropertiesImpl.of(propList);
		client.publishWith().topic("test/nrdoug").qos(MqttQos.AT_LEAST_ONCE).payload(payload.getBytes()).userProperties(props).send();
		client.disconnect();
		pause(600L);
	}

	private class SubscriberThread extends Thread {

		Mqtt5BlockingClient client = MqttClient.builder().identifier(UUID.randomUUID().toString())
				.serverHost(hivehost)
				.useMqttVersion5()
				.buildBlocking();
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

	private class PublisherThread extends Thread {

		int count = 0;

		Mqtt5BlockingClient client = MqttClient.builder().identifier(UUID.randomUUID().toString())
				.serverHost(hivehost)
				.useMqttVersion5()
				.buildBlocking();

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
}
