package com.nr.paho.mqtt5.examples;

import java.util.Random;

import org.eclipse.paho.mqttv5.client.MqttClient;
import org.eclipse.paho.mqttv5.client.persist.MemoryPersistence;
import org.eclipse.paho.mqttv5.common.MqttException;

import com.newrelic.api.agent.NewRelic;
import com.newrelic.api.agent.Trace;
import com.newrelic.api.agent.TransactionNamePriority;

public class SyncPublisher implements Runnable {

	private MqttClient client = null;
	private static long duration = 15*60*1000L;
	private String hivehost = "localhost";
	private String clientName = "Tester-Publisher";

	public static void main(String[] args) {
		SyncPublisher app = new SyncPublisher(args);

		app.run();
	}
	
	public SyncPublisher(String[] args) {
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
                    if(arg.startsWith("-duration")) {
                            int index = arg.indexOf('=');
                            if(index > -1) {
                                    String maxStr = arg.substring(index+1);
                                    if(!maxStr.isEmpty()) {
                                            try {
                                                    duration = Long.parseLong(maxStr);
                                            } catch(NumberFormatException e) {
                                                    System.out.println("Unable to parse duration from "+maxStr);
                                            }
                                    }
                            }
                    }
                    if(arg.startsWith("-client")) {
                        int index = arg.indexOf('=');
                        if(index > -1) {
                                String clientStr = arg.substring(index+1);
                                clientName = clientStr;
                        }
                    	
                    }
            }
    }
		
	}

	public void run() {
		try {
			init();
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
		client = new MqttClient(url, clientName, new MemoryPersistence());
		client.connect();
	}

	private static final Random random = new Random();
	private static final int MAX = 15;

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
