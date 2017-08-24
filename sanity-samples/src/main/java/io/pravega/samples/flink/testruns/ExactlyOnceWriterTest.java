/*
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 */

package io.pravega.samples.flink.testruns;

/*
 * Standard Flink Job using in-memory source generator and Pravega sync
 * What it does?
 *		- Runs a Flink Job that will read from the in-memory source generator (Running Integer Counter test Data)
 *			and writes to Pravega output stream using FlinkExactlyOncePravegaWriter
 *		- The source will wait after a threshold is reached for a checkpoint to happen
 *		- An identity mapper in the pipeline will simulate a failure upon a threshold whiich will cause the job to restart
 *		- Restart will start from last successful checkpoint and the job should resume
 *		- Pravega should not be writing any duplicate values or invalid counts of data
 *
 */

import io.pravega.connectors.flink.util.FlinkPravegaParams;
import io.pravega.connectors.flink.util.StreamId;
import io.pravega.samples.flink.EventCounterApp;
import io.pravega.samples.flink.StreamUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExactlyOnceWriterTest {

	private static final Logger log = LoggerFactory.getLogger(ExactlyOnceWriterTest.class);

	public static void main(String[] args) {

		log.info("Starting ExactlyOnceWriterTest Main...");

		ParameterTool params = ParameterTool.fromArgs(args);
		log.info("Parameter Tool: {}", params.toMap());

		/*
		  All arguments are optional.
		  --controller <PRAVEGA_CONTROLLER_ID>
		  --segments <TOTAL_SEGMENTS>
		  --parallelism <FLINK_PARALLELISM>
		  --outStream <SCOPE/OUTPUT_STREAM_NAME>
		  --validateResults <true|false>
		  --numElements <totalEvents>
		 */

		FlinkPravegaParams flinkPravegaParams = new FlinkPravegaParams(params);
		final String controllerUri = flinkPravegaParams.getControllerUri().toString();
		int numElements = params.getInt("numElements", 100000);
		boolean validateResults = params.getBoolean("validateResults", true);

		StreamUtils streamUtils = new StreamUtils(flinkPravegaParams);
		StreamId streamId = streamUtils.createStream("outStream");
		try {
			EventCounterApp eventCounterApp = new EventCounterApp();
			eventCounterApp.exactlyOnceWriteSimulator(streamId, streamUtils, numElements);

		} catch (Exception e) {
			log.error("Exception occurred", e);
		}

		if (ExactlyOnceWriterTest.class.getClassLoader().getClass().getName().contains("AppClassLoader")) {
			log.info("Exiting ExactlyOnceWriterTest Main...");

			if (validateResults) {
				log.info("Validating results...");
				try {
					streamUtils.validateJobOutputResults(streamId.getName(), streamId.getScope(), numElements, controllerUri);
				} catch (Exception e) {
					log.error("Failed to verify the sink results", e);
				}
			}

			System.exit(0);
		}
	}

}
