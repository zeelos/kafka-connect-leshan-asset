/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.zeelos.leshan.kafka.connect.sink.asset.utils;

import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Collection;
import java.util.Properties;

public final class ConnectorUtils {

    private static final Logger log = LoggerFactory.getLogger(ConnectorUtils.class);

    private static String version = "unknown";

    static {
        try {
            Properties props = new Properties();
            props.load(ConnectorUtils.class.getResourceAsStream("/leshan-connect-asset-version.properties"));
            version = props.getProperty("version", version).trim();
        } catch (Exception e) {
            log.warn("Error while loading version:", e);
        }
    }

    private ConnectorUtils() {
    }

    public static String getVersion() {
        return version;
    }

    public static String loadBanner(String filename) {
        StringBuilder sb = new StringBuilder();

        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(ConnectorUtils.class.getResourceAsStream("/" + filename)))) {

            String line;
            while ((line = reader.readLine()) != null) {
                sb.append(line).append(System.lineSeparator());
            }

            return sb.toString();
        } catch (IOException e) {/*ignore*/}

        return sb.toString();
    }

    public static void logCoordinates(String name, Collection<SinkRecord> records) {
        if (log.isDebugEnabled() && records.size() > 0) {
            final SinkRecord first = records.iterator().next();
            final int recordsCount = records.size();
            log.debug("['{}'] [{}] received {} records with first record kafka coordinates:(topic:{},partition:{},offset:{})",
                    name, Thread.currentThread().getName(), recordsCount, first.topic(), first.kafkaPartition(), first.kafkaOffset());
        }
    }
}