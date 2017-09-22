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

import io.pravega.client.stream.Serializer
import java.nio.ByteBuffer

object ByteArraySerializer : Serializer<ByteArray> {
    override fun deserialize(buf: ByteBuffer): ByteArray {
        if (buf.hasArray() && buf.arrayOffset() == 0 && buf.position() == 0 && buf.limit() == buf.capacity()) {
            return buf.array()
        }
        else {
            val bytes = ByteArray(buf.remaining())
            buf.get(bytes)
            return bytes
        }
    }

    override fun serialize(value: ByteArray): ByteBuffer {
        return ByteBuffer.wrap(value)
    }
}