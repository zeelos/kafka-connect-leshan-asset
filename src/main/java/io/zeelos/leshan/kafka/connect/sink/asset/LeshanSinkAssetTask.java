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

import io.zeelos.leshan.avro.registration.*;
import io.zeelos.leshan.kafka.connect.sink.asset.utils.ConnectorUtils;
import io.zeelos.leshan.kafka.connect.sink.asset.utils.TopicAvroMapper;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.errors.RetriableException;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;
import org.apache.tinkerpop.gremlin.orientdb.OrientGraphFactory;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class LeshanSinkAssetTask extends SinkTask {

    private static Logger log = LoggerFactory.getLogger(LeshanSinkAssetTask.class);

    private LeshanSinkAssetConnectorConfig config;

    private Graph graph;

    private int remainingRetries, remainingRetriesReset;
    private long retryBackoffMs;

    private boolean shouldReconnect;


    @Override
    public String version() {
        return ConnectorUtils.getVersion();
    }

    @Override
    public void start(Map<String, String> settings) {
        log.info(ConnectorUtils.loadBanner("leshan-sink-asset-ascii.txt"));

        this.config = new LeshanSinkAssetConnectorConfig(settings);

        remainingRetriesReset = config.getInt(LeshanSinkAssetConnectorConfig.ORIENTDB_MAX_RETRIES);
        remainingRetries = remainingRetriesReset;

        retryBackoffMs = this.config.getLong(LeshanSinkAssetConnectorConfig.ORIENTDB_RETRY_BACKOFF_MS);

        initGraphConnection();
    }

    private void initGraphConnection() {
        try {
            // close (if any) prior "timeout" graph connections
            if (graph != null) {
                graph.close();
            }

            String tinkerPop3Url = config.getString(LeshanSinkAssetConnectorConfig.TINKERPOP3_URL_CONFIG);

            this.graph = new OrientGraphFactory(tinkerPop3Url,
                    config.getString(LeshanSinkAssetConnectorConfig.TINKERPOP3_USERNAME_CONFIG),
                    config.getPassword(LeshanSinkAssetConnectorConfig.TINKERPOP3_PASSWORD_CONFIG).value())
                    .getNoTx();
            this.shouldReconnect = false; // reset flag

            log.info("Connected to TinkerPop3 database '{}'", tinkerPop3Url);

        } catch (Exception e) {
            throw new ConnectException(e);
        }
    }

    @Override
    public void put(Collection<SinkRecord> records) {
        if (records.size() == 0) // nothing to do if empty
            return;

        ConnectorUtils.logCoordinates(config.CONNECTOR_NAME, records);

        // reconnect if we were asked to do so, due to a db connection reset and connect task backoff
        if (shouldReconnect) {
            initGraphConnection();
        }

        records.forEach(record -> {
            log.debug("['{}'] [{}] processing new message with payload: '{}'", config.CONNECTOR_NAME, Thread.currentThread().getName(), record);

            try {
                AvroRegistrationResponse response = TopicAvroMapper.REGISTRATION.fromConnectRecord(record, AvroRegistrationResponse.class);

                switch (response.getKind()) {
                    case NEW:
                        AvroRegistrationNew regNew = (AvroRegistrationNew) response.getBody();
                        handleRegistrationNew(regNew);
                        break;
                    case UPDATE:
                        AvroRegistrationUpdate regUp = (AvroRegistrationUpdate) response.getBody();
                        handleRegistrationUpdate(regUp);
                        break;
                    case DELETE:
                        AvroRegistrationDelete regDel = (AvroRegistrationDelete) response.getBody();
                        handleRegistrationDelete(regDel);
                        break;
                }
            } catch (LeshanServerNotFoundException | EndpointNotFoundException e) {
                log.warn("an error occurred during processing of record: {}", e.getMessage());
            } catch (com.orientechnologies.common.io.OIOException e) {
                log.warn("write of {} records failed due to a connection error to db, remainingRetries={}", records.size(), remainingRetries, e);
                if (remainingRetries == 0) {
                    throw new ConnectException("unable to recover from connection to db, aborting..", e);
                } else {
                    remainingRetries--;
                    shouldReconnect = true;

                    context.timeout(retryBackoffMs);

                    throw new RetriableException(e);
                }
            } catch (Exception e) {
                throw new ConnectException(e);
            }
        });
        remainingRetries = remainingRetriesReset;
    }

    private void handleRegistrationNew(AvroRegistrationNew registration) {
        GraphTraversalSource traversal = graph.traversal();

        String ep = registration.getEp();
        String serverId = registration.getServerId();

        Vertex server = traversal.V().hasLabel("Servers")
                .has("name", serverId)
                .tryNext().orElseGet(
                        () -> graph.addVertex(T.label, "Servers", "name", serverId));

        // if there is an existing endpoint, remove it first
        traversal.V().hasLabel("Endpoints").has("name", ep).tryNext().ifPresent(Element::remove);
        Vertex endpoint = graph.addVertex(T.label, "Endpoints",
                "name", ep,
                "regDate", registration.getRegDate(),
                "regId", registration.getRegId(),
                "regPort", registration.getRegPort(),
                "regAddress", registration.getRegAddr(),
                "port", registration.getPort(),
                "address", registration.getAddress(),
                "lt", registration.getLt(),
                "expired", Boolean.FALSE,
                "ver", registration.getVer(),
                "bnd", registration.getBnd(),
                "regLastUpdate", registration.getLastUp(),
                "online", Boolean.TRUE
        );

        // optional attributes that may exist
        Optional.ofNullable(registration.getSms()).ifPresent((sms) -> endpoint.property("sms", sms));
        Optional.ofNullable(registration.getAttributes()).ifPresent((attributes) -> endpoint.property("attributes", attributes));

        List<AvroLink> links = Optional.ofNullable(registration.getLinks()).orElseGet(/*an empty list */ArrayList::new);
        handleRegistrationLinks(links, endpoint, traversal);

        server.addEdge("hasEndpoint", endpoint);
    }

    private void handleRegistrationUpdate(AvroRegistrationUpdate registration) {
        GraphTraversalSource traversal = graph.traversal();

        String ep = registration.getEp();
        String serverId = registration.getServerId();

        Vertex server = traversal.V().hasLabel("Servers")
                .has("name", serverId)
                .tryNext().orElseThrow(() -> new LeshanServerNotFoundException(String.format("received registration update for endpoint '%s' attached to an unknown server '%s'", ep, serverId)));

        Vertex endpoint = traversal.V().hasLabel("Endpoints")
                .has("name", ep)
                .tryNext().orElseThrow(() -> new EndpointNotFoundException(String.format("received registration update for endpoint '%s' without a prior new registration", ep)));

        endpoint.property("regId", registration.getRegId());
        endpoint.property("port", registration.getPort());
        endpoint.property("address", registration.getAddress());
        endpoint.property("regLastUpdate", registration.getLastUp());
        endpoint.property("online", Boolean.TRUE);

        // optional attributes that may exist.
        Optional.ofNullable(registration.getLt()).ifPresent((lt) -> endpoint.property("lt", lt));
        Optional.ofNullable(registration.getSms()).ifPresent((sms) -> endpoint.property("sms", sms));
        Optional.ofNullable(registration.getBnd()).ifPresent((bnd) -> endpoint.property("bnd", bnd));
        Optional.ofNullable(registration.getAttributes()).ifPresent((attributes) -> endpoint.property("attributes", attributes));

        List<AvroLink> links = Optional.ofNullable(registration.getLinks()).orElseGet(/*an empty list */ArrayList::new);
        handleRegistrationLinks(links, endpoint, traversal);
    }

    private void handleRegistrationDelete(AvroRegistrationDelete registration) {
        GraphTraversalSource traversal = graph.traversal();

        String ep = registration.getEp();

        Vertex endpoint = traversal.V().hasLabel("Endpoints")
                .has("name", ep)
                .tryNext().orElseThrow(() -> new EndpointNotFoundException(String.format("received registration delete for endpoint '%s' without a prior new registration", ep)));

        endpoint.property("regLastUpdate", registration.getLastUp());
        endpoint.property("expired", registration.getExpired());
        endpoint.property("online", Boolean.FALSE);
    }

    private boolean checkifVertexAndEdgePropertyExists(Iterator<Edge> elements, Direction direction, Vertex element, String idfield, String edgeField, Object edgeValue) {
        while (elements.hasNext()) {
            Edge edge = elements.next();
            Vertex v = ((direction == Direction.OUT) ?
                    edge.inVertex() :
                    edge.outVertex());

            if (v.value(idfield).equals(element.value(idfield))
                    && edge.value(edgeField).equals(edgeValue))
                return true;
        }
        return false;
    }

    private void handleRegistrationLinks(List<AvroLink> links, Vertex endpoint, GraphTraversalSource traversal) {
        for (AvroLink link : links) {
            String url = link.getUrl();
            // parse {objectId/instanceId}
            String[] arr = url.split("/");

            if (url.equals("/") || arr.length != 3) // TODO: care for missing/invalid arguments
                continue;

            int objectId = Integer.parseInt(arr[1]);
            int instanceId = Integer.parseInt(arr[2]);

            // we assume that the LWM2M model has already been initialized.
            // if the Object Id is not found, we warn and skip this record
            Optional<Vertex> object = traversal.V().hasLabel("Objects")
                    .has("id", objectId)
                    .tryNext();

            if (object.isPresent()) {
                Iterator<Edge> edges = endpoint.edges(Direction.OUT, "hasObject");
                if (!checkifVertexAndEdgePropertyExists(edges, Direction.OUT, object.get(), "id", "instance", instanceId)) {
                    endpoint.addEdge("hasObject", object.get(), "instance", instanceId);
                }
            } else {
                log.warn("endpoint '{}' references object with id '{}' which was not found in the existing supporting model, please initialize it in the database, skipping this object..", endpoint.property("name").value(), objectId);
            }
        }
    }

    @Override
    public void flush(Map<TopicPartition, OffsetAndMetadata> map) {
    }

    @Override
    public void stop() {
        log.info("Stopping LeshanSinkAssetTask");

        try {
            if (graph != null) {
                graph.close();

                log.info("Disconnected from TinkerPop3 database");
            }
        } catch (Exception e) {
            // TODO: better handling
        }
    }


    class LeshanServerNotFoundException extends IllegalStateException {
        LeshanServerNotFoundException(String msg) {
            super(msg);
        }
    }

    class EndpointNotFoundException extends IllegalStateException {
        EndpointNotFoundException(String msg) {
            super(msg);
        }
    }
}