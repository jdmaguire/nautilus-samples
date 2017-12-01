/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */

package io.pravega.samples.flink;

import io.pravega.client.ClientFactory;
import io.pravega.client.admin.ReaderGroupManager;
import io.pravega.client.stream.*;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.connectors.flink.FlinkPravegaWriter;
import io.pravega.connectors.flink.PravegaEventRouter;
import io.pravega.connectors.flink.PravegaWriterMode;
import io.pravega.connectors.flink.serialization.PravegaSerialization;
import io.pravega.connectors.flink.util.FlinkPravegaParams;
import io.pravega.connectors.flink.util.StreamId;
import org.apache.commons.lang3.RandomStringUtils;
import org.apache.flink.streaming.util.serialization.SerializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.net.URI;
import java.util.BitSet;
import java.util.Collections;
import java.util.UUID;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

public class StreamUtils {

	private static final Logger log = LoggerFactory.getLogger(StreamUtils.class);

	public static final int DEFAULT_NUM_SEGMENTS = 1;

	private final String scope = "scope";

	private final int numSegments;

	final FlinkPravegaParams flinkPravegaParams;

	public StreamUtils(FlinkPravegaParams flinkPravegaParams) {
		this(flinkPravegaParams, DEFAULT_NUM_SEGMENTS);
	}

	public StreamUtils(FlinkPravegaParams flinkPravegaParams, int numSegments) {
		this.flinkPravegaParams = flinkPravegaParams;
		this.numSegments = numSegments;
	}

	public String getScope() { return scope; }

	public int getNumSegments() { return numSegments; }

	public FlinkPravegaParams getFlinkPravegaParams() { return flinkPravegaParams; }

	public URI getControllerUri() { return flinkPravegaParams.getControllerUri(); }

	public void publishData(final StreamId streamId, final int numElements) {
		final EventStreamWriter<Integer> eventWriter = createWriter(streamId.getName(), streamId.getScope());
		for (int i=1; i<=numElements; i++) {
			eventWriter.writeEvent(i);
		}
		eventWriter.close();
	}

	public StreamId createStream(final String streamParamName) {
		final String defaultStreamName = RandomStringUtils.randomAlphabetic(20);
		StreamId streamId = flinkPravegaParams.createStreamFromParam(streamParamName, scope + "/" + defaultStreamName);
		log.info("Created stream: {} with scope: {}", streamId.getName(), streamId.getScope());
		return streamId;
	}

	public EventStreamWriter<Integer> createWriter(final String streamName, final String scope) {
		ClientFactory clientFactory = ClientFactory.withScope(scope, flinkPravegaParams.getControllerUri());
		return clientFactory.createEventWriter(
				streamName,
				new JavaSerializer<>(),
				EventWriterConfig.builder().build());
	}

	// TODO move these methods to FlinkPravegaParams class
	public <T extends Serializable> FlinkPravegaWriter<T> newExactlyOnceWriter(final StreamId stream,
																	final Class<T> eventType,
																	final PravegaEventRouter<T> router) {
		return newExactlyOnceWriter(stream, PravegaSerialization.serializationFor(eventType), router);
	}

	public <T extends Serializable> FlinkPravegaWriter<T> newExactlyOnceWriter(final StreamId stream,
																			   final SerializationSchema<T> serializationSchema,
																			   final PravegaEventRouter<T> router) {
		FlinkPravegaWriter writer = new FlinkPravegaWriter<T>(getControllerUri(), stream.getScope(), stream.getName(), serializationSchema, router);
		writer.setPravegaWriterMode(PravegaWriterMode.EXACTLY_ONCE);
		return writer;
	}


	public void validateJobOutputResults(String outputStream, String defaultScope, int numElements, String controllerUri) throws Exception {

		try (EventStreamReader<Integer> reader = getIntegerReader(outputStream,
				defaultScope,
				URI.create(controllerUri))) {

			final BitSet duplicateChecker = new BitSet();

			for (int numElementsRemaining = numElements; numElementsRemaining > 0;) {
				final EventRead<Integer> eventRead = reader.readNextEvent(1000);
				final Integer event = eventRead.getEvent();

				if (event != null) {
					//log.info("event -> {}", event);
					numElementsRemaining--;
					assertFalse("found a duplicate", duplicateChecker.get(event));
					duplicateChecker.set(event);
				}
			}

			// no more events should be there
			assertNull("too many elements written", reader.readNextEvent(1000).getEvent());

			reader.close();
		}

	}

	public EventStreamReader<Integer> getIntegerReader(final String streamName,
														final String scope,
														final URI controllerUri) {
		ReaderGroupManager readerGroupManager = ReaderGroupManager.withScope(scope, controllerUri);
		final String readerGroup = "testReaderGroup" + scope + streamName;
		readerGroupManager.createReaderGroup(
				readerGroup,
				ReaderGroupConfig.builder().startingTime(0).build(),
				Collections.singleton(streamName));

		ClientFactory clientFactory = ClientFactory.withScope(scope, controllerUri);
		final String readerGroupId = UUID.randomUUID().toString();
		return clientFactory.createReader(
				readerGroupId,
				readerGroup,
				new JavaSerializer<>(),
				ReaderConfig.builder().build());
	}


}
