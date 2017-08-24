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
 * Standard Flink Job using Pravega as source and sync
 * What it does?
 * 		- Create Streams (input/output),
 * 		- Publish Running Integer Counter test Data using Pravega EventStreamWriter,
 *		- Runs a Flink Job that will read from the input stream using FlinkPravegaReader
 *			and writes to Pravega output stream using FlinkPravegaWriter
 * Can be used to test happy path integration
 */

import io.pravega.connectors.flink.util.FlinkPravegaParams;
import io.pravega.connectors.flink.util.StreamId;
import io.pravega.samples.flink.EventCounterApp;
import io.pravega.samples.flink.StreamUtils;
import org.apache.flink.api.java.utils.ParameterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExactlyOnceReaderWriterTest {

	private static final Logger log = LoggerFactory.getLogger(ExactlyOnceReaderWriterTest.class);

	public static void main(String[] args) {

		log.info("Starting ExactlyOnceReaderWriterTest Main...");

		ParameterTool params = ParameterTool.fromArgs(args);
		log.info("Parameter Tool: {}", params.toMap());

		/*
		  All arguments are optional.
		  --controller <PRAVEGA_CONTROLLER_ID>
		  --segments <TOTAL_SEGMENTS>
		  --parallelism <FLINK_PARALLELISM>
		  --inStream <SCOPE/INPUT_STREAM_NAME>
		  --outStream <SCOPE/OUTPUT_STREAM_NAME>
		  --validateResults <true|false>
		  --numElements <totalEvents>
		 */

		FlinkPravegaParams flinkPravegaParams = new FlinkPravegaParams(params);
		final String controllerUri = flinkPravegaParams.getControllerUri().toString();
		int numElements = params.getInt("numElements", 10000);
		boolean validateResults = params.getBoolean("validateResults", true);
		boolean generateData = true;
		boolean throttle = true;

		StreamUtils streamUtils = new StreamUtils(flinkPravegaParams);
		StreamId inStreamId = streamUtils.createStream("inStream");
		StreamId outStreamId = streamUtils.createStream("outStream");

		try {
			EventCounterApp eventCounterApp = new EventCounterApp();
			eventCounterApp.exactlyOnceReadWriteSimulator(inStreamId, outStreamId, streamUtils, numElements, generateData, throttle);
		} catch (Exception e) {
			log.error("Exception occurred", e);
		}

		if (ExactlyOnceReaderWriterTest.class.getClassLoader().getClass().getName().contains("AppClassLoader")) {
			log.info("Exiting ExactlyOnceReaderWriterTest Main...");

			if (validateResults) {
				log.info("Validating results...");
				try {
					streamUtils.validateJobOutputResults(outStreamId.getName(), outStreamId.getScope(), numElements, controllerUri);
				} catch (Exception e) {
					log.error("Failed to verify the sink results", e);
				}
			}


			System.exit(0);
		}
	}

}
