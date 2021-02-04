package com.nr.fit.hivemq.client.examples;

import java.util.UUID;

import com.hivemq.client.mqtt.MqttClient;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5RxClient;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5PublishResult;
import com.newrelic.api.agent.Trace;

import io.reactivex.Flowable;
import io.reactivex.Single;


public class RxClient_Pub {

	static int max = 300;
	private String hivehost = "localhost";


	public static void main(String[] args) {
		RxClient_Pub sClient = new RxClient_Pub(args);
		sClient.initialize();
	}

	public RxClient_Pub(String[] args) {
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

		pThread.start();
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
	public void publish(Mqtt5RxClient client, String payload) {
		System.out.println("Call to publish("+payload+")");
			client.connect().blockingGet();
			Mqtt5Publish pub = Mqtt5Publish.builder().topic("test/nrdoug-rx").qos(MqttQos.AT_LEAST_ONCE).payload(payload.getBytes()).build();
			
			Single<Mqtt5PublishResult> single = client.publish(Flowable.just(pub)).singleOrError();
			Mqtt5PublishResult pubResult = single.blockingGet();
			
			System.out.println("Publish Result "+pubResult);
			client.disconnect().blockingAwait();
		pause(600L);
	}


	private class PublisherThread extends Thread {

		int count = 0;

		Mqtt5RxClient client = MqttClient.builder().identifier(UUID.randomUUID().toString())
				.serverHost(hivehost)
				.useMqttVersion5()
				.buildBlocking()
				.toRx();

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
