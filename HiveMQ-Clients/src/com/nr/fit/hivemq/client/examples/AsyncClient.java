package com.nr.fit.hivemq.client.examples;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.function.Consumer;

import com.hivemq.client.mqtt.MqttClient;
import com.hivemq.client.mqtt.MqttGlobalPublishFilter;
import com.hivemq.client.mqtt.datatypes.MqttQos;
import com.hivemq.client.mqtt.mqtt5.Mqtt5AsyncClient;
import com.hivemq.client.mqtt.mqtt5.message.connect.connack.Mqtt5ConnAck;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5Publish;
import com.hivemq.client.mqtt.mqtt5.message.publish.Mqtt5PublishResult;
import com.hivemq.client.mqtt.mqtt5.message.subscribe.suback.Mqtt5SubAck;
import com.newrelic.api.agent.Trace;


public class AsyncClient {

	static int max = 300;
	
	private String hivehost = "localhost";

	public static void main(String[] args) {
		AsyncClient sClient = new AsyncClient(args);
		sClient.initialize();
	}

	public AsyncClient(String[] args) {
		if(args.length > 0) {
			for(int i=0;i<args.length;i++) {
				String arg = args[i];
				System.out.println("processing arg "+arg);
				
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
		System.out.println("Connected to HiveMQ at "+hivehost+", will publish "+max+" messages");
	}

	public void initialize() {
		PublisherThread pThread = new PublisherThread();
		SubscriberThread sThread = new SubscriberThread();

		pThread.start();
		sThread.start();
	}

	@Trace(dispatcher=true)
	public void subscribe(Mqtt5AsyncClient client, int c) {
		System.out.println("Call to subscribe, count = "+c);
		final Listener listener = new Listener();
		
		try {
		
		CompletableFuture<Mqtt5ConnAck> f = client.connect();
		Mqtt5ConnAck ack = f.get();
		System.out.println("Connected: "+ack);
		
		CompletableFuture<Mqtt5SubAck> f2 = client.subscribeWith().topicFilter("test/nrdoug-async").qos(MqttQos.EXACTLY_ONCE).send();

		f2.get();

		Consumer<Mqtt5Publish> consumer = new Consumer<Mqtt5Publish>() {
			
			@Override
			public void accept(Mqtt5Publish publish) {
				System.out.println("Received message from "+publish.getTopic()+ ", payload: "+new String(publish.getPayloadAsBytes()));
				listener.setDone();
			}
		};
			
		
		client.publishes(MqttGlobalPublishFilter.ALL,consumer);
		
	
		} catch (InterruptedException e) {
				e.printStackTrace();
				client.disconnect();
		} catch (ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		while(!listener.isDone()) {
			try {
				Thread.sleep(10L);
			} catch (InterruptedException e) {
			}
		}
		try {
			client.disconnect().get();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	@Trace(dispatcher=true)
	public void subscribeEx(Mqtt5AsyncClient client, int c, Executor executor) {
		System.out.println("Call to subscribe, count = "+c);
		final Listener listener = new Listener();
		
		try {
		
		CompletableFuture<Mqtt5ConnAck> f = client.connect();
		Mqtt5ConnAck ack = f.get();
		System.out.println("Connected: "+ack);
		
		CompletableFuture<Mqtt5SubAck> f2 = client.subscribeWith().topicFilter("test/nrdoug2-async").qos(MqttQos.EXACTLY_ONCE).send();

		f2.get();

		Consumer<Mqtt5Publish> consumer = new Consumer<Mqtt5Publish>() {
			
			@Override
			public void accept(Mqtt5Publish publish) {
				System.out.println("Received message from "+publish.getTopic()+ ", payload: "+new String(publish.getPayloadAsBytes()));
				listener.setDone();
			}
		};
			
		
		client.publishes(MqttGlobalPublishFilter.ALL,consumer,executor);
		
	
		} catch (InterruptedException e) {
				e.printStackTrace();
				client.disconnect();
		} catch (ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		while(!listener.isDone()) {
			try {
				Thread.sleep(10L);
			} catch (InterruptedException e) {
			}
		}
		try {
			client.disconnect().get();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
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
	public void publish(Mqtt5AsyncClient client, String payload) {
		System.out.println("Call to publish("+payload+")");
		try {
			client.connect().get();
			CompletableFuture<Mqtt5PublishResult> pubResult = client.publishWith().topic("test/nrdoug-async").qos(MqttQos.AT_LEAST_ONCE).payload(payload.getBytes()).send();
			pubResult.get();
			client.disconnect().get();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		pause(600L);
	}

	@Trace(dispatcher=true)
	public void publishEx(Mqtt5AsyncClient client, String payload) {
		System.out.println("Call to publish("+payload+")");
		try {
			client.connect().get();
			CompletableFuture<Mqtt5PublishResult> pubResult = client.publishWith().topic("test/nrdoug2-async").qos(MqttQos.AT_LEAST_ONCE).payload(payload.getBytes()).send();
			pubResult.get();
			client.disconnect().get();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ExecutionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		pause(600L);
	}

	private class SubscriberThread extends Thread {
		
		Executor executor = Executors.newSingleThreadExecutor();

		Mqtt5AsyncClient client = MqttClient.builder().identifier(UUID.randomUUID().toString())
				.serverHost(hivehost)
				.useMqttVersion5()
				.buildBlocking().toAsync();
		int count = 0;

		public SubscriberThread() {
			super("SubscriberThread");
		}

		@Override
		public void run() {
			while(count < max) {

				subscribe(client, count);
				pause(100L);
				subscribeEx(client, count, executor);
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
	private class PublisherThread extends Thread {

		int count = 0;

		Mqtt5AsyncClient client = MqttClient.builder().identifier(UUID.randomUUID().toString())
				.serverHost(hivehost)
				.useMqttVersion5()
				.buildBlocking()
				.toAsync();

		public PublisherThread() {
			super("PublisherThread");
		}

		@Override
		public void run() {
			while(count < max) {
				publish(client,Integer.toString(count));
				pause(200L);
				publishEx(client, Integer.toString(count));
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
