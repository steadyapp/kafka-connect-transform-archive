/**
 * Copyright © 2017 Jeremy Custenborder (jcustenborder@gmail.com)
 *
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
package com.github.jcustenborder.kafka.connect.archive;

import com.github.jcustenborder.kafka.connect.utils.config.Description;
import com.github.jcustenborder.kafka.connect.utils.config.DocumentationNote;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.transforms.Transformation;

import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.requireStructOrNull;
import static org.apache.kafka.connect.transforms.util.Requirements.requireMapOrNull;

@Description("The Archive transformation is used to help preserve all of the data for a message when archived to S3.")
@DocumentationNote("This transform works by copying the key, value, topic, and timestamp to new record where this is all " +
        "contained in the value of the message. This will allow connectors like Confluent's S3 connector to properly archive " +
        "the record.")
public class Archive<R extends ConnectRecord<R>> implements Transformation<R> {

  private static final String PURPOSE_KEY = "Message key enclosed into a field";
  private static final String PURPOSE_VALUE = "Message value enclosed into a field";

  private Cache<String, Schema> schemaUpdateCache;

  @Override
  public R apply(R r) {
    if (r.valueSchema() == null && r.keySchema() == null) {
      return applySchemaless(r);
    } else {
      return applyWithSchema(r);
    }
  }

  private Schema makeUpdatedSchema(Schema keySchema, Schema valueSchema) {

    return SchemaBuilder.struct()
            .name("com.github.jcustenborder.kafka.connect.archive.Storage")
            .field("key", keySchema).optional().defaultValue(null)
            .field("value", valueSchema).optional().defaultValue(null)
            .field("topic", Schema.STRING_SCHEMA)
            .field("timestamp", Schema.INT64_SCHEMA);
  }

  private R applyWithSchema(R r) {
    String cacheKey = String.format("%s-key", r.topic());
    String cacheValue = String.format("%s-value", r.topic());
    Schema cachedKeySchema = schemaUpdateCache.get(cacheKey);
    Schema cachedValueSchema = schemaUpdateCache.get(cacheValue);

    Schema keySchema = r.keySchema();
    Schema valueSchema = r.valueSchema();
    final Struct recordKey = requireStructOrNull(r.key(), PURPOSE_KEY);
    final Struct recordValue = requireStructOrNull(r.value(), PURPOSE_VALUE);

    if (keySchema == null) {
      keySchema = (cachedKeySchema != null ? cachedKeySchema : Schema.OPTIONAL_STRING_SCHEMA);
    } else {
      // last schema wins
      schemaUpdateCache.put(cacheKey, keySchema);
    }

    if (valueSchema == null) {
      valueSchema = (cachedValueSchema != null ? cachedValueSchema : Schema.OPTIONAL_STRING_SCHEMA);
    } else {
      // last schema wins
      schemaUpdateCache.put(cacheValue, valueSchema);
    }

    Schema schema = makeUpdatedSchema(keySchema, valueSchema);
    Struct value = new Struct(schema)
            .put("topic", r.topic())
            .put("timestamp", r.timestamp());
    if (recordKey != null) {
      value.put("key", recordKey);
    }
    if (recordValue != null) {
      value.put("value", recordValue);
    }

    return r.newRecord(r.topic(), r.kafkaPartition(), r.keySchema(), r.key(), schema, value, r.timestamp());
  }

  @SuppressWarnings("unchecked")
  private R applySchemaless(R r) {

    final Map<String, Object> archiveValue = new HashMap<>();

    final Map<String, Object> recordKey = requireMapOrNull(r.key(), PURPOSE_KEY);
    final Map<String, Object> recordValue = requireMapOrNull(r.value(), PURPOSE_VALUE);

    archiveValue.put("key", recordKey);
    archiveValue.put("value", recordValue);
    archiveValue.put("topic", r.topic());
    archiveValue.put("timestamp", r.timestamp());

    return r.newRecord(r.topic(), r.kafkaPartition(), r.keySchema(), r.key(), null, archiveValue, r.timestamp());
  }

  @Override
  public ConfigDef config() {
    return new ConfigDef();
  }

  @Override
  public void close() {
  }

  @Override
  public void configure(Map<String, ?> map) {
    schemaUpdateCache = new SynchronizedCache<>(new LRUCache<>(16));
  }
}