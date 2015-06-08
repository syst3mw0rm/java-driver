/*
 *      Copyright (C) 2012-2015 DataStax Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package com.datastax.driver.core;

import org.testng.annotations.Test;

import static com.datastax.driver.core.Assertions.assertThat;
import static com.datastax.driver.core.HostDistance.LOCAL;

public class PoolingOptionsTest {

    @Test(groups = "unit")
    public void should_set_core_and_max_connections_simultaneously() {
        PoolingOptions options = new PoolingOptions();

        options.setConnectionsPerHost(LOCAL, 10, 15);

        assertThat(options.getCoreConnectionsPerHost(LOCAL)).isEqualTo(10);
        assertThat(options.getMaxConnectionsPerHost(LOCAL)).isEqualTo(15);
    }
}
