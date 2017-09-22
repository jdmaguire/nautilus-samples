/**
 * Copyright (c) 2017 Dell Inc., or its subsidiaries. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 */
package io.pravega.ingest

import io.pravega.client.ClientFactory
import io.pravega.client.admin.StreamManager
import io.pravega.client.stream.EventStreamWriter
import io.pravega.client.stream.EventWriterConfig
import io.pravega.client.stream.ScalingPolicy
import io.pravega.client.stream.StreamConfiguration
import java.net.URI
import java.util.concurrent.ConcurrentHashMap
import javax.ws.rs.*
import javax.ws.rs.core.Response

@Path("/")
class IngestResource(controllerUri: String) {
    private val controllerUri = URI(controllerUri)
    private val cachedWriters = ConcurrentHashMap<String, EventStreamWriter<ByteArray>>()

    @POST
    @Path("{scope}/{stream}")
    fun ingest(@PathParam("scope") scope: String,
               @PathParam("stream") stream: String,
               @QueryParam("key") key: String?,
               body: ByteArray): Response {
        val writer = cachedWriters.getOrPut("$scope/$stream") {
            // Create scope
            val streamManager = StreamManager.create(controllerUri)
            streamManager.createScope(scope)

            // Create stream
            val config = StreamConfiguration.builder()
                    .scope(scope)
                    .streamName(stream)
                    .scalingPolicy(ScalingPolicy.fixed(1))
                    .build()
            streamManager.createStream(scope, stream, config)

            val clientFactory = ClientFactory.withScope(scope, controllerUri)
            val eventWriterConfig = EventWriterConfig.builder().build()

            clientFactory.createEventWriter(stream, ByteArraySerializer, eventWriterConfig)
        }
        writer.writeEvent(key, body)

        return Response.ok().build()
    }
}
