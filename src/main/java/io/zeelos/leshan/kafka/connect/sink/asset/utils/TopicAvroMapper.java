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

import io.confluent.connect.avro.AvroData;
import io.confluent.connect.avro.AvroDataConfig;
import io.zeelos.leshan.avro.registration.AvroRegistrationResponse;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.kafka.connect.sink.SinkRecord;

import java.io.ByteArrayOutputStream;

public enum TopicAvroMapper {

    REGISTRATION(AvroRegistrationResponse.getClassSchema());

    private final GenericDatumWriter<GenericRecord> writer;
    private static final AvroData avroData;

    static {
        AvroDataConfig config = new AvroDataConfig.Builder()
                .with(AvroDataConfig.ENHANCED_AVRO_SCHEMA_SUPPORT_CONFIG, true)
                .with(AvroDataConfig.CONNECT_META_DATA_CONFIG, false)
                .build();

        avroData = new AvroData(config);
    }

    TopicAvroMapper(org.apache.avro.Schema schema) {
        this.writer = new GenericDatumWriter<>(schema);
    }

    public byte[] fromConnectRecord(SinkRecord record) throws Exception {
        GenericRecord obj = (GenericRecord) avroData.fromConnectData(record.valueSchema(), record.value());
        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        BinaryEncoder binEncoder = EncoderFactory.get().binaryEncoder(bout, null);
        writer.write(obj, binEncoder);

        binEncoder.flush();

        return bout.toByteArray();
    }

    public <V> V fromConnectRecord(SinkRecord record, Class<V> cls) throws Exception {
        byte[] bytes = fromConnectRecord(record);

        SpecificDatumReader<V> reader = new SpecificDatumReader<V>(cls);

        BinaryDecoder binDecoder = DecoderFactory.get().binaryDecoder(bytes, null);
        return reader.read(null, binDecoder);
    }
}