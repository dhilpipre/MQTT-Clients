package com.nr.paho.mqtt5.examples;

import org.eclipse.paho.mqttv5.client.IMqttMessageListener;
import org.eclipse.paho.mqttv5.client.IMqttToken;
import org.eclipse.paho.mqttv5.client.MqttClient;
import org.eclipse.paho.mqttv5.client.persist.MemoryPersistence;
import org.eclipse.paho.mqttv5.common.MqttException;
import org.eclipse.paho.mqttv5.common.MqttSubscription;

public class SyncSubcriber implements Runnable {

	private MqttClient client = null;
	private static long duration = 15*60*1000L;
	private String hivehost = "localhost";
	private String clientName = "Tester-Subscriber";
	MessageListener listener;

	public static void main(String[] args) {
		SyncSubcriber app = new SyncSubcriber(args);

		app.run();
	}
	
	public SyncSubcriber(String[] args) {
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
			subscribe();
			pauseSeconds(3);
			long start = System.currentTimeMillis();

			long running = System.currentTimeMillis() - start;
			while(running < duration) {
				pauseSeconds(10);
				
				running = listener.getCurrentDuration();
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

	public void subscribe() throws MqttException {
		MqttSubscription subscription = new MqttSubscription(Utils.TOPIC, 2);
		listener = new MessageListener();
		IMqttToken result = client.subscribe(new MqttSubscription[] {subscription}, new IMqttMessageListener[] {listener});
		result.waitForCompletion();
	}


	public void close() throws MqttException {
		client.disconnect();
	}

	public void pauseSeconds(int n) {
		pause(n*1000L);
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
