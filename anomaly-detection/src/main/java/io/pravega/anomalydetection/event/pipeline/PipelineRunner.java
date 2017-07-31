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
		producer.setLatencyInMilliSec(params.getLong("producer-latency", 2L));
		producer.setCapacity(params.getInt("producer-capacity", 100));
		producer.setErrorProbFactor(params.getDouble("producer-error-factor", 0.3));
		producer.setControlledEnv(params.getBoolean("producer-controlled", false));

		AppConfiguration.ElasticSearch elasticSearch = new AppConfiguration.ElasticSearch();
		elasticSearch.setSinkResults(params.getBoolean("elastic-sink", true));
		elasticSearch.setHost(params.get("elastic-host", "master.elastic.l4lb.thisdcos.directory"));
		elasticSearch.setPort(params.getInt("elastic-port", 9300));
		elasticSearch.setCluster(params.get("elastic-cluster", "elastic"));
		elasticSearch.setIndex(params.get("elastic-index", "anomaly-index"));
		elasticSearch.setType(params.get("elastic-type", "anomalies"));

		AppConfiguration.Pipeline pipeline = new AppConfiguration.Pipeline();
		pipeline.setCheckpointIntervalInMilliSec(params.getLong("pipeline-checkpoint-interval", 1000L));
		pipeline.setDisableCheckpoint(params.getBoolean("pipeline-disable-checkpoint", false));
		pipeline.setWatermarkOffsetInSec(params.getInt("pipeline-watermark-offset", 0));
		pipeline.setWindowIntervalInSeconds(params.getInt("pipeline-window-interval", 5));
		pipeline.setElasticSearch(elasticSearch);

		appConfiguration = new AppConfiguration();
		appConfiguration.setName(params.get("name", "anomaly-detection"));
		appConfiguration.setProducer(producer);
		appConfiguration.setPipeline(pipeline);

		runMode = params.get("mode", "processor");

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
