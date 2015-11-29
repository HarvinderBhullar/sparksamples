package com.hs.datacollection;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;

public class CustomHttpReceiver extends Receiver<String> {

	private final String API_ROOT = "http://machinepark.actyx.io/api/v1";

	public CustomHttpReceiver() {
		super(StorageLevel.MEMORY_AND_DISK_2());
	}

	public void onStart() {
		new Thread() {
			@Override
			public void run() {
				receive();
			}
		}.start();
	}

	@Override
	public void onStop() {
		// TODO Auto-generated method stub

	}

	/** Create a connection and receive data until receiver is stopped */
	private void receive() {
		try {
			try {
				String[] urls = getURLList();
				for (int j = 0; j < urls.length; j++) {
					store(getData(urls[j]));
				}
			} finally {
			}
			// Restart in an attempt to connect again when server is active
			// again
			restart("Trying to connect again");
		} catch (Exception ce) {
			// restart if could not connect to server
			restart("Could not connect", ce);
		} catch (Throwable t) {
			restart("Error receiving data", t);
		}
	}

	private String getData(String url) throws Exception {
		Client client = Client.create();
		WebResource webResource = client.resource(url);
		ClientResponse response = webResource.accept("application/json").get(ClientResponse.class);
		if (response.getStatus() != 200) {
			throw new RuntimeException("Failed : HTTP error code : " + response.getStatus());
		}
		String[] d = url.split("/");
		String machineId = d[d.length -1 ];
        return  response.getEntity(String.class).replace("}", ", \"machine_id\":\"" + machineId + "\"}");
	}

	private String[] getURLList() {
		Client client = Client.create();
		WebResource webResource = client.resource("http://machinepark.actyx.io/api/v1/machines");
		ClientResponse response = webResource.accept("application/json").get(ClientResponse.class);
		if (response.getStatus() != 200) {
			throw new RuntimeException("Failed : HTTP error code : " + response.getStatus());
		}
		String output = response.getEntity(String.class);
		output = output.replace("[", "").replace("]", "").replaceAll("\\$API_ROOT", API_ROOT).replaceAll("\"", "");
		return output.split(",");
	}

}
