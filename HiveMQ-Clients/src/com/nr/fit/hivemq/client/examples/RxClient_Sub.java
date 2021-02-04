package com.nr.fit.hivemq.client.examples;

import java.util.Random;
import java.util.UUID;

import com.hivemq.client.mqtt.MqttClient;
import com.hivemq.client.mqtt.MqttGlobalPublishFilter;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5RxClient;
import com.hivemq.client.mqtt.mqtt5.message.connect.connack.Mqtt5ConnAck;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import com.hivemq.client.mqtt.mqtt5.message.subscribe.suback.Mqtt5SubAck;
import com.newrelic.api.agent.Trace;

import io.reactivex.Flowable;
import io.reactivex.Single;


public class RxClient_Sub {

	static int max = 200;
	static final Random random = new Random();
	private String hivehost = "localhost";

	public static void main(String[] args) {
		RxClient_Sub sClient = new RxClient_Sub(args);
		sClient.initialize();
	}

	public RxClient_Sub(String[] args) {
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
		SubscriberThread sThread = new SubscriberThread();

		sThread.start();
	}

	@Trace(dispatcher=true)
	public void subscribe(Mqtt5RxClient client, int c) {
		System.out.println("Call to subscribe, count = "+c);
		final Listener listener = new Listener();
		
		
		Single<Mqtt5ConnAck> f = client.connect();
		Mqtt5ConnAck ack = f.blockingGet();
		System.out.println("Connected: "+ack);
		
		Single<Mqtt5SubAck> f2 = client.subscribeWith().topicFilter("test/nrdoug-rx").qos(MqttQos.EXACTLY_ONCE).applySubscribe();

		Mqtt5SubAck subAck = f2.blockingGet();
		System.out.println("subscribe ack: "+subAck);
		
		Flowable<Mqtt5Publish> f3 = client.publishes(MqttGlobalPublishFilter.ALL);
		
		f3 = f3.doOnNext(publish -> {
			processMessage(publish);
			listener.setDone();

		}
		);
		
		Mqtt5Publish pub = f3.blockingFirst();
		System.out.println("Publish result is "+pub);
		while(!listener.isDone()) {
			try {
				Thread.sleep(10L);
			} catch (InterruptedException e) {
			}
		}
			client.disconnect().blockingAwait();
	}
	
	private void pauseRandomUnits() {
		int n = random.nextInt(20);
		if(n > 0) {
			try {
				Thread.sleep(n*100L);
			} catch(Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	@Trace
	public void processMessage(Mqtt5Publish publish) {
		System.out.println("Received message from "+publish.getTopic()+ ", payload: "+new String(publish.getPayloadAsBytes()));
		pauseRandomUnits();
	}
	
	private class SubscriberThread extends Thread {
		
		Mqtt5RxClient client = MqttClient.builder().identifier(UUID.randomUUID().toString())
				.serverHost(hivehost)
				.useMqttVersion5()
				.buildBlocking().toRx();
		
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

	private class Listener {
		
		boolean done;
		
		Listener() {
			done = false;
		}
		
		public void setDone() {
			done = true;
		}
		
		public boolean isDone() {
			return done;
		}
	}
}
