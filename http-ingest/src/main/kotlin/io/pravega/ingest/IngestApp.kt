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

import io.dropwizard.Application
import io.dropwizard.setup.Environment

class IngestApp : Application<IngestAppConfig>() {

    override fun run(configuration: IngestAppConfig, env: Environment) {
        val ingestResource = IngestResource(configuration.pravegaControllerUri)

        env.jersey().register(ingestResource)
    }

    companion object {
        @JvmStatic fun main(args: Array<String>) {
            IngestApp().run(*args)
        }
    }
}