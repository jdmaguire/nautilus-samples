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

import io.pravega.client.stream.*;
import io.pravega.connectors.flink.FlinkExactlyOncePravegaWriter;
import io.pravega.connectors.flink.FlinkPravegaReader;
import io.pravega.connectors.flink.FlinkPravegaWriter;
import io.pravega.connectors.flink.PravegaWriterMode;
import io.pravega.connectors.flink.PravegaEventRouter;
import io.pravega.connectors.flink.util.StreamId;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicReference;

import static io.pravega.samples.flink.EventCounterApp.NotifyingMapper.TO_CALL_ON_COMPLETION;

public class EventCounterApp {

	private static final Logger log = LoggerFactory.getLogger(EventCounterApp.class);

	public static final int DEFAULT_PARALLELISM = 1;

	private final int parallelism;

	public EventCounterApp() {
		this(DEFAULT_PARALLELISM);
	}

	public EventCounterApp(int parallelism) {
		this.parallelism = parallelism;
	}

	public void exactlyOnceWriteSimulator(final StreamId outStreamId, final StreamUtils streamUtils, int numElements) throws Exception {

		final int checkpointInterval = 100;

		final int restartAttempts = 1;
		final long delayBetweenAttempts = 0L;

		//30 sec timeout for all
		final long txTimeout = 30 * 1000;
		final long txTimeoutMax = 30 * 1000;
		final long txTimeoutGracePeriod = 30 * 1000;

		final String jobName = "ExactlyOnceSimulator";

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(parallelism);

		env.enableCheckpointing(checkpointInterval);
		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(restartAttempts, delayBetweenAttempts));

		// Pravega Writer
		FlinkExactlyOncePravegaWriter<Integer> pravegaExactlyOnceWriter = streamUtils.newExactlyOnceWriter(outStreamId,
				Integer.class, new IdentityRouter<>(),
				txTimeout, txTimeoutMax, txTimeoutGracePeriod);

		env
				.addSource(new IntegerCounterSourceGenerator(numElements))
				.map(new FailingIdentityMapper<>(numElements / parallelism / 2))
				.rebalance()
				.addSink(pravegaExactlyOnceWriter);

		env.execute(jobName);
	}

	public void exactlyOnceReadWriteSimulator(final StreamId inStreamId, final StreamId outStreamId,
											  final StreamUtils streamUtils, int numElements,
											  boolean generateData, boolean throttled) throws Exception {

		final int blockAtNum = numElements/2;
		final int sleepPerElement = 1;

		final int checkpointInterval = 100;
		final int taskFailureRestartAttempts = 3;
		final long delayBetweenRestartAttempts = 0L;
		final long startTime = 0L;
		final String jobName = "exactlyOnceReadWriteSimulator";

		//30 sec timeout for all
		final long txTimeout = 30 * 1000;
		final long txTimeoutMax = 30 * 1000;
		final long txTimeoutGracePeriod = 30 * 1000;

		EventStreamWriter<Integer> eventWriter;
		ThrottledIntegerWriter producer = null;

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(parallelism);
		env.enableCheckpointing(checkpointInterval);
		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(taskFailureRestartAttempts, delayBetweenRestartAttempts));

		// we currently need this to work around the case where tasks are started too late, a checkpoint was already triggered, and some tasks
		// never see the checkpoint event
		env.getCheckpointConfig().setCheckpointTimeout(2000);

		// the Pravega reader
		final FlinkPravegaReader<Integer> pravegaSource = streamUtils.getFlinkPravegaParams().newReader(inStreamId, startTime, Integer.class);

		// Pravega Writer
		FlinkExactlyOncePravegaWriter<Integer> pravegaExactlyOnceWriter = streamUtils.newExactlyOnceWriter(outStreamId,
				Integer.class, new IdentityRouter<>(),
				txTimeout, txTimeoutMax, txTimeoutGracePeriod);

		DataStream<Integer> stream =
		env.addSource(pravegaSource)
				.map(new FailingIdentityMapper<>(numElements * 2 / 3))
				.setParallelism(1)

				.map(new NotifyingMapper<>())
				.setParallelism(1);

				stream.addSink(pravegaExactlyOnceWriter)
				.setParallelism(1);

				stream.addSink(new IntSequenceExactlyOnceValidator(numElements))
				.setParallelism(1);

		if (generateData) {
			eventWriter = streamUtils.createWriter(inStreamId.getName(), inStreamId.getScope());
			producer = new ThrottledIntegerWriter(eventWriter, numElements, blockAtNum, sleepPerElement, false);
			producer.start();
			if (throttled) {
				ThrottledIntegerWriter finalProducer = producer;
				TO_CALL_ON_COMPLETION.set(() -> finalProducer.unThrottle());
			}
		}

		try {
			env.execute(jobName);
		} catch (Exception e) {
			if (!(ExceptionUtils.getRootCause(e) instanceof IntSequenceExactlyOnceValidator.SuccessException)) {
				throw e;
			}
		}

		if (generateData && producer != null) producer.sync();

	}

	public void standardReadWriteSimulator(final StreamId inStreamId, final StreamId outStreamId, final StreamUtils streamUtils, int numElements) throws Exception {

		final int checkpointInterval = 100;
		final int taskFailureRestartAttempts = 1;
		final long delayBetweenRestartAttempts = 0L;
		final long startTime = 0L;
		final String jobName = "standardReadWriteSimulator";

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(parallelism);
		env.enableCheckpointing(checkpointInterval);
		env.setRestartStrategy(RestartStrategies.fixedDelayRestart(taskFailureRestartAttempts, delayBetweenRestartAttempts));

		// the Pravega reader
		final FlinkPravegaReader<Integer> pravegaSource = streamUtils.getFlinkPravegaParams().newReader(inStreamId, startTime, Integer.class);

		// Pravega Writer
		FlinkPravegaWriter<Integer> pravegaWriter = streamUtils.getFlinkPravegaParams().newWriter(outStreamId, Integer.class, new IdentityRouter<>());
		pravegaWriter.setPravegaWriterMode(PravegaWriterMode.ATLEAST_ONCE);

		DataStream<Integer> stream = env.addSource(pravegaSource).map(new IdentityMapper<>());

		stream.addSink(pravegaWriter);

		stream.addSink(new IntSequenceExactlyOnceValidator(numElements));

		env.execute(jobName);

	}

	private static class IdentityRouter<T> implements PravegaEventRouter<T> {
		@Override
		public String getRoutingKey(T event) {
			return String.valueOf(event);
		}
	}

	public static class NotifyingMapper<T> implements MapFunction<T, T>, CheckpointListener {

		public static final AtomicReference<Runnable> TO_CALL_ON_COMPLETION = new AtomicReference<>();

		@Override
		public T map(T element) throws Exception {
			return element;
		}

		@Override
		public void notifyCheckpointComplete(long l) throws Exception {
			Runnable r = TO_CALL_ON_COMPLETION.get();
			// throttled enabled
			if (r != null) {
				TO_CALL_ON_COMPLETION.get().run();
			}
		}
	}

	public static class IdentityMapper<T> implements MapFunction<T, T> {
		@Override
		public T map(T value) throws Exception {
			return value;
		}
	}

}