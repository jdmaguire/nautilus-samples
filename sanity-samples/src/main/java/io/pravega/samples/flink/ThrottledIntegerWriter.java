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

package io.pravega.samples.flink;

import io.pravega.client.stream.EventStreamWriter;
import org.apache.flink.core.testutils.CheckedThread;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThrottledIntegerWriter extends CheckedThread implements AutoCloseable {

	private static final Logger log = LoggerFactory.getLogger(ThrottledIntegerWriter.class);

	private final EventStreamWriter<Integer> eventWriter;

	private final int numValues;

	private final int blockAtNum;

	private final int sleepPerElement;

	private final Object blocker = new Object();

	private volatile boolean throttled;

	private volatile boolean running;

	public ThrottledIntegerWriter(EventStreamWriter<Integer> eventWriter,
								  int numValues, int blockAtNum, int sleepPerElement, boolean throttled) {

		super("ThrottledIntegerWriter");

		this.eventWriter = eventWriter;
		this.numValues = numValues;
		this.blockAtNum = blockAtNum;
		this.sleepPerElement = sleepPerElement;

		this.running = true;
		this.throttled = throttled;
	}

	@Override
	public void go() throws Exception {
		// emit the sequence of values
		for (int i = 1; running && i <= numValues; i++) {

			// throttle speed if still requested
			// if we reach the 'blockAtNum' element before being un-throttled,
			// we need to wait until we are un-throttled
			if (throttled) {
				if (i < blockAtNum) {
					Thread.sleep(sleepPerElement);
				} else {
					synchronized (blocker) {
						while (running && throttled) {
							blocker.wait();
						}
					}
				}
			} else {
				if (i < blockAtNum) {
					Thread.sleep(sleepPerElement);
				} else {
					Thread.sleep(1);
				}
			}

			eventWriter.writeEvent(String.valueOf(i), i);
		}
	}

	public void unThrottle() {
		synchronized (blocker) {
			throttled = false;
			blocker.notifyAll();
		}
	}

	@Override
	public void close() {
		this.running = false;
		interrupt();
	}

}
