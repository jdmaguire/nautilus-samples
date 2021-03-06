/**
 *
 * Copyright (c) 2017 Dell Inc., or its subsidiaries.
 *
 */
package io.pravega.anomalydetection.event.state;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;

public class EventsGenerator {

	private static final Logger LOG = LoggerFactory.getLogger(EventsGenerator.class);

	private static final List<String> cityNames = Lists.newArrayList(City.CITIES.keySet());
	//private double errorProb = 0.0000001;

	private final double errorProb;

	private final int initialCapacity;

	private Random rnd = new Random();

	private Map<Integer, EventStateMachine.State> states = new LinkedHashMap<>();

	public EventsGenerator(int initialCapacity, double errorProb) {
		this.initialCapacity = initialCapacity;
		this.errorProb = errorProb;
	}

	public int numActiveEntries() {
		return states.size();
	}

	/*
	 * @param minIp The lower bound for the range from which a new IP address may be picked.
	 * @param maxIp The upper bound for the range from which a new IP address may be picked.
	 * @return A next random
	 */
	public Event next(int minIp, int maxIp) {
		double p = rnd.nextDouble();

		Event returnEvent = null;

		if (p * initialCapacity >= states.size()) {
			// create a new state machine
			int nextIP = rnd.nextInt(maxIp - minIp) + minIp;

			if (!states.containsKey(nextIP)) {
				Tuple2<Event.EventType, EventStateMachine.State> stateTransitionTuple = EventStateMachine.Transitions.initialState.randomTransition(rnd);
				states.put(nextIP, stateTransitionTuple.f1);
				String networkId = getNetworkId();
				returnEvent = new Event(nextIP, stateTransitionTuple.f0, getCurrentTime(), networkId,getLatLon(networkId));
			}
			else {
				// collision on IP address, try again
				next(minIp, maxIp);
			}
		}
		else {
			// pick an existing state machine

			// skip over some elements in the linked map, then take the next
			// update it, and insert it at the end

			int numToSkip = Math.min(20, rnd.nextInt(states.size()));
			Iterator<Map.Entry<Integer,EventStateMachine.State>> iter = states.entrySet().iterator();
			int i = 0;
			while (i < numToSkip) {
				i += 1;
				iter.next();
			}

			Map.Entry<Integer,EventStateMachine.State> entry = iter.next();
			int address = entry.getKey();
			EventStateMachine.State currentState = entry.getValue();
			iter.remove();

			if (p < errorProb) {
				Event.EventType event = currentState.randomInvalidTransition(rnd);
				String networkId = getNetworkId();
				returnEvent = new Event(address, event, getCurrentTime(), networkId,getLatLon(networkId));
				LOG.info("**** Emitting invalid event: [{}], ", returnEvent);
			}
			else {
				Tuple2<Event.EventType, EventStateMachine.State> stateTransitionTuple = currentState.randomTransition(rnd);
				if (!stateTransitionTuple.f1.terminal()) {
					// reinsert
					states.put(address, stateTransitionTuple.f1);
				}
				String networkId = getNetworkId();
				returnEvent = new Event(address, stateTransitionTuple.f0, getCurrentTime(), networkId,getLatLon(networkId));
			}
		}

		return returnEvent;
	}

	/**
	 * Creates an event for an illegal state transition of one of the internal
	 * state machines. If the generator has not yet started any state machines
	 * (for example, because no call to [[next(Int, Int)]] was made, yet), this
	 * will return [[None]].
	 *
	 * @return An event for a illegal state transition, or [[None]], if not possible.
	 */
	public Optional<Event> nextInvalid() {
		Iterator<Map.Entry<Integer,EventStateMachine.State>>  iter = states.entrySet().iterator();
		if (iter.hasNext()) {
			Map.Entry<Integer,EventStateMachine.State> entry = iter.next();
			int address = entry.getKey();
			EventStateMachine.State currentState = entry.getValue();
			iter.remove();

			Event.EventType event = currentState.randomInvalidTransition(rnd);
			String networkId = getNetworkId();
			Event e = new Event(address, event, getCurrentTime(), networkId,getLatLon(networkId));
			return Optional.of(e);
		}
		return Optional.empty();
	}

	private Instant getCurrentTime() {
		return Instant.now();
	}

	private String getNetworkId() {
		int randomNum = ThreadLocalRandom.current().nextInt(0, cityNames.size() - 1);
		return cityNames.get(randomNum);
	}

	private Event.LatLon getLatLon(String networkId) {
		return City.CITIES.get(networkId);
	}
}