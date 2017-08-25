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
package io.pravega.anomalydetection.event.pipeline;

import io.pravega.anomalydetection.event.AppConfiguration;
import io.pravega.connectors.flink.util.FlinkPravegaParams;
import org.apache.flink.api.java.utils.ParameterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

public class PipelineRunner {

	private static final Logger LOG = LoggerFactory.getLogger(PipelineRunner.class);

	private AppConfiguration appConfiguration;
	private FlinkPravegaParams pravega;
	private String runMode;

	private void parseConfigurations(String[] args) {
		LOG.info("ApplicationMain Main.. Arguments: {}", Arrays.asList(args));

		ParameterTool params = ParameterTool.fromArgs(args);
		LOG.info("Parameter Tool: {}", params.toMap());

		AppConfiguration.Producer producer = new AppConfiguration.Producer();

		// producer-latency: How frequently events are generated and published to Pravega.
		producer.setLatencyInMilliSec(params.getLong("producer-latency", 250L));

		// producer-capacity: Initial capacity till which the error records will not be generated
		producer.setCapacity(params.getInt("producer-capacity", 100));

		// producer-error-factor: How frequently error records needs to be simulated. A value between 0.0 and 1.0.
		producer.setErrorProbFactor(params.getDouble("producer-error-factor", 0.3));

		// producer-controlled: When this is true, the value of `producer-error-factor` will be ignored and
		//                      error records will be generated when you press ENTER in a console.
		producer.setControlledEnv(params.getBoolean("producer-controlled", false));

		AppConfiguration.ElasticSearch elasticSearch = new AppConfiguration.ElasticSearch();

		// elastic-sink: Whether to sink the results to Elastic Search or not.
		elasticSearch.setSinkResults(params.getBoolean("elastic-sink", true));

		// elastic-host: Host of the Elastic instance to sink to.
		elasticSearch.setHost(params.get("elastic-host", "master.elastic.l4lb.thisdcos.directory"));

		// elastic-port: Port of the Elastic instance to sink to.
		elasticSearch.setPort(params.getInt("elastic-port", 9300));

		// elastic-cluster: The name of the Elastic cluster to sink to.
		elasticSearch.setCluster(params.get("elastic-cluster", "elastic"));

		// elastic-index: The name of the Elastic index to sink to.
		elasticSearch.setIndex(params.get("elastic-index", "anomaly-index"));

		// elastic-type: The name of the type to sink.
		elasticSearch.setType(params.get("elastic-type", "anomalies"));

		AppConfiguration.Pipeline pipeline = new AppConfiguration.Pipeline();

		// pipeline-checkpoint-interval: Flink checkpointing interval in milliseconds.
		pipeline.setCheckpointIntervalInMilliSec(params.getLong("pipeline-checkpoint-interval", 1000L));

		// pipeline-disable-checkpoint: Disable Flink checkpointing.
		pipeline.setDisableCheckpoint(params.getBoolean("pipeline-disable-checkpoint", false));

		// pipeline-watermark-offset: Window watermark offset interval.
		pipeline.setWatermarkOffsetInSec(params.getInt("pipeline-watermark-offset", 0));

		// pipeline-window-interval: Window frequency interval in seconds
		pipeline.setWindowIntervalInSeconds(params.getInt("pipeline-window-interval", 5));

		pipeline.setElasticSearch(elasticSearch);

		appConfiguration = new AppConfiguration();

		// name: Name of the Flink application. The mode is appended when not using the default.
		appConfiguration.setName(params.get("name", "anomaly-detection"));
		appConfiguration.setProducer(producer);
		appConfiguration.setPipeline(pipeline);

		// mode: The mode to the Flink application in. Defaults to processor, but can also
		runMode = params.get("mode", "processor");

		// controller: The pravega controller endpoint to connect to.
		// stream: The pravega stream to use (<scope>/<name>). If not specified, defaults to "examples/NetworkPacket"
		pravega = new FlinkPravegaParams(ParameterTool.fromArgs(args));
	}

	private void printUsage() {
		StringBuilder message = new StringBuilder();
		message.append("\n############################################################################################################\n");
		message.append("Usage: io.pravega.anomalydetection.ApplicationMain --mode <producer or processor>").append("\n");
		message.append("producer  == Publish streaming events to Pravega").append("\n");
		message.append("processor == Run Anomaly Detection by reading from Pravega stream").append("\n");
		message.append("############################################################################################################");
		LOG.error("{}", message.toString());
	}


	public void run(String[] args) {
		parseConfigurations(args);

		try {
			new StreamCreator(appConfiguration, pravega).run();

			AbstractPipeline pipeline = null;
			switch (runMode) {
				case "producer":
					LOG.info("Running event publisher to publish events to Pravega stream");
					pipeline = new PravegaEventPublisher(appConfiguration, pravega);
					break;
				case "processor":
					new ElasticSetup(appConfiguration).run();

					LOG.info("Running anomaly detection by reading from Pravega stream");
					pipeline = new PravegaAnomalyDetectionProcessor(appConfiguration, pravega);
					break;
				default:
					LOG.error("Incorrect run mode [{}] specified", runMode);
					printUsage();
					System.exit(-1);
			}
			pipeline.run();
		} catch (Exception e) {
			LOG.error("Failed to run the pipeline.", e);
		}

	}
}
