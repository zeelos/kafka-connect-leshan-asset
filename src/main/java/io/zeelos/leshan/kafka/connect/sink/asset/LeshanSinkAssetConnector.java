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

import io.zeelos.leshan.kafka.connect.sink.asset.utils.ConnectorUtils;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class LeshanSinkAssetConnector extends SinkConnector {

    private static Logger log = LoggerFactory.getLogger(LeshanSinkAssetConnector.class);

    private Map<String, String> settings;

    @Override
    public String version() {
        return ConnectorUtils.getVersion();
    }

    @Override
    public void start(Map<String, String> settings) {
        this.settings = settings;
    }

    @Override
    public Class<? extends Task> taskClass() {
        return LeshanSinkAssetTask.class;
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        log.info("Setting task configurations for {} sink asset workers.", maxTasks);

        return Collections.nCopies(maxTasks, this.settings);
    }

    @Override
    public void stop() {
        log.info("Stopping LeshanSinkAssetConnector");
    }

    @Override
    public ConfigDef config() {
        return LeshanSinkAssetConnectorConfig.CONFIG;
    }
}
