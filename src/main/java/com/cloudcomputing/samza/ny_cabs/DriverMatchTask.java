package com.cloudcomputing.samza.ny_cabs;

import org.apache.samza.config.Config;
import org.apache.samza.system.IncomingMessageEnvelope;
import org.apache.samza.system.OutgoingMessageEnvelope;
import org.apache.samza.task.InitableTask;
import org.apache.samza.task.MessageCollector;
import org.apache.samza.task.StreamTask;
import org.apache.samza.task.TaskContext;
import org.apache.samza.task.TaskCoordinator;
import org.apache.samza.task.WindowableTask;

import com.google.gson.Gson;
import org.apache.samza.storage.kv.*;

import java.util.HashSet;

/**
 * Consumes the stream of driver location updates and rider cab requests.
 * Outputs a stream which joins these 2 streams and gives a stream of rider to
 * driver matches.
 */
public class DriverMatchTask implements StreamTask, InitableTask, WindowableTask {
	
	public static class Data{
		public long driverId;
		public double latitude;
		public double longitude;
		public char gender;
		public char gender_preference;
		public double salary;
		public double rating;
		public long blockId;
		public String status;
		public String type;
	}
    /* Define per task state here. (kv stores etc) */
	private KeyValueStore<Long, String> driverInfo;
	private KeyValueStore<Long, HashSet<Long>> blockDriver;
    private double MAX_MONEY = 100.0;
    private String LEAVING_BLOCK = "LEAVING_BLOCK";
    private String ENTERING_BLOCK = "ENTERING_BLOCK";
    private String RIDE_REQUEST = "RIDE_REQUEST";
    private String RIDE_COMPLETE = "RIDE_COMPLETE";
    @Override
    @SuppressWarnings("unchecked")
    public void init(Config config, TaskContext context) throws Exception {
        // Initialize (maybe the kv stores?)
    	driverInfo = (KeyValueStore<Long, String>) context.getStore("driver-loc");
    	blockDriver = (KeyValueStore<Long, String) context.getStore("block-driver");
    }

    @Override
    @SuppressWarnings("unchecked")
    public void process(IncomingMessageEnvelope envelope, MessageCollector collector, TaskCoordinator coordinator) {
        // The main part of your code. Remember that all the messages for a
        // particular partition
        // come here (somewhat like MapReduce). So for task 1 messages for a
        // blockId will arrive
        // at one task only, thereby enabling you to do stateful stream
        // processing.
        String incomingStream = envelope.getSystemStreamPartition().getStream();
        Gson gson = new Gson();
        if (incomingStream.equals(DriverMatchConfig.DRIVER_LOC_STREAM.getStream())) {
        	// Handle Driver Location messages
        	Data data = gson.fromJson(envelope.getMessage().toString(), Data.class);
        	blockDriver.get(data.blockId).add(data.driverId);
        	driverInfo.put(data.driverId, data.toString());
        } else if (incomingStream.equals(DriverMatchConfig.EVENT_STREAM.getStream())) {
        	// Handle Event messages
        	Data data = gson.fromJson(envelope.getMessage().toString(), Data.class);
        	if (data.type.equals(RIDE_COMPLETE)) {
        		
        	} else if (data.type.equals(RIDE_REQUEST)) {
        		
        	} else if (data.type.equals(ENTERING_BLOCK)) {
        		
        	} else if (data.type.equals(LEAVING_BLOCK)) {
        		
        	}
        } else {
            throw new IllegalStateException("Unexpected input stream: " + envelope.getSystemStreamPartition());
        }
    }

    @Override
    public void window(MessageCollector collector, TaskCoordinator coordinator) {
        // this function is called at regular intervals, not required for this
        // project
    }
}
