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

package io.zeelos.leshan.kafka.connect.sink.asset;

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;

import java.util.Map;

public class LeshanSinkAssetConnectorConfig extends AbstractConfig {

    public static final String TINKERPOP3_URL_CONFIG = "tinkerpop3.url";
    public static final String TINKERPOP3_USERNAME_CONFIG = "tinkerpop3.user";
    public static final String TINKERPOP3_PASSWORD_CONFIG = "tinkerpop3.password";
    public static final String ORIENTDB_MAX_RETRIES = "orientdb.max.retries";
    public static final int ORIENTDB_MAX_RETRIES_DEFAULT = 3;
    public static final String ORIENTDB_RETRY_BACKOFF_MS = "orientdb.retry.backoff.ms";
    public static final long ORIENTDB_RETRY_BACKOFF_MS_DEFAULT = 5000;

    public final String CONNECTOR_NAME;

    protected static ConfigDef config() {
        return new ConfigDef()
                .define(TINKERPOP3_URL_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH,
                        "The url to connect to the TinkerPop3 database", "TinkerPop3", 1, ConfigDef.Width.LONG, "TinkerPop3 Server URL")
                .define(TINKERPOP3_USERNAME_CONFIG, ConfigDef.Type.STRING, ConfigDef.Importance.HIGH,
                        "Username to authenticate to TinkerPop3 database.", "TinkerPop3", 2, ConfigDef.Width.LONG, "Username")
                .define(TINKERPOP3_PASSWORD_CONFIG, ConfigDef.Type.PASSWORD, ConfigDef.Importance.HIGH,
                        "Password to authenticate to TinkerPop3 database.", "TinkerPop3", 3, ConfigDef.Width.LONG, "Password")
                .define(ORIENTDB_MAX_RETRIES, ConfigDef.Type.INT, ORIENTDB_MAX_RETRIES_DEFAULT, ConfigDef.Importance.MEDIUM,
                        "The maximum number of times to retry on errors before failing the task.", "OrientDB", 1, ConfigDef.Width.MEDIUM, "Maximum Retries")
                .define(ORIENTDB_RETRY_BACKOFF_MS, ConfigDef.Type.LONG, ORIENTDB_RETRY_BACKOFF_MS_DEFAULT, ConfigDef.Importance.MEDIUM,
                        "The time in milliseconds to wait following an error before a retry attempt is made.", "OrientDB", 2, ConfigDef.Width.MEDIUM, "Retry Backoff (millis)");
    }

    public static final ConfigDef CONFIG = config();

    public LeshanSinkAssetConnectorConfig(Map<String, String> originals) {
        super(CONFIG, originals);

        CONNECTOR_NAME = originals.get("name");
    }
}
