/*
 * Copyright 2015-2019 The OpenZipkin Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package zipkin2.storage.cassandra;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.Version;
import com.datastax.oss.driver.api.core.metadata.Node;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.Serializable;
import java.net.InetAddress;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import zipkin2.Annotation;
import zipkin2.Endpoint;
import zipkin2.internal.Nullable;

import static java.nio.charset.StandardCharsets.UTF_8;
import static zipkin2.storage.cassandra.CassandraUtil.resourceToString;

final class Schema {
  static final Logger LOG = LoggerFactory.getLogger(Schema.class);

  static final String TABLE_SPAN = "span";
  static final String TABLE_TRACE_BY_SERVICE_SPAN = "trace_by_service_span";
  static final String TABLE_TRACE_BY_SERVICE_REMOTE_SERVICE = "trace_by_service_remote_service";
  static final String TABLE_SERVICE_SPANS = "span_by_service";
  static final String TABLE_SERVICE_REMOTE_SERVICES = "remote_service_by_service";
  static final String TABLE_DEPENDENCY = "dependency";
  static final String TABLE_AUTOCOMPLETE_TAGS = "autocomplete_tags";

  static final String DEFAULT_KEYSPACE = "zipkin2";
  static final String SCHEMA_RESOURCE = "/zipkin2-schema.cql";
  static final String INDEX_RESOURCE = "/zipkin2-schema-indexes.cql";
  static final String UPGRADE_1 = "/zipkin2-schema-upgrade-1.cql";
  static final String UPGRADE_2 = "/zipkin2-schema-upgrade-2.cql";

  Schema() {
  }

  static Metadata readMetadata(CqlSession session) {
    KeyspaceMetadata keyspaceMetadata =
      ensureKeyspaceMetadata(session, session.getKeyspace().map(CqlIdentifier::asInternal).get());

    Map<String, String> replication = keyspaceMetadata.getReplication();
    if ("SimpleStrategy".equals(replication.get("class"))) {
      if ("1".equals(replication.get("replication_factor"))) {
        LOG.warn("running with RF=1, this is not suitable for production. Optimal is 3+");
      }
      //DriverConfig config = session.getContext().getConfig();
      //ConsistencyLevel cl =
      //  session.getCluster().getConfiguration().getQueryOptions().getConsistencyLevel();
      //
      //checkState(
      //  ConsistencyLevel.ONE == cl, "Do not define `local_dc` and use SimpleStrategy");
    }
    String compactionClass = "asd";
    //keyspaceMetadata.getTable("span").getOptions().getCompaction().get("class");

    boolean hasAutocompleteTags = hasUpgrade1_autocompleteTags(keyspaceMetadata);
    if (!hasAutocompleteTags) {
      LOG.warn(
        "schema lacks autocomplete indexing: apply {}, or set CassandraStorage.ensureSchema=true",
        UPGRADE_1);
    }

    boolean hasRemoteService = hasUpgrade2_remoteService(keyspaceMetadata);
    if (!hasRemoteService) {
      LOG.warn(
        "schema lacks remote service indexing: apply {}, or set CassandraStorage.ensureSchema=true",
        UPGRADE_2);
    }

    return new Metadata(compactionClass, hasAutocompleteTags, hasRemoteService);
  }

  static final class Metadata {
    final String compactionClass;
    final boolean hasAutocompleteTags, hasRemoteService;

    Metadata(String compactionClass, boolean hasAutocompleteTags,
      boolean hasRemoteService) {
      this.compactionClass = compactionClass;
      this.hasAutocompleteTags = hasAutocompleteTags;
      this.hasRemoteService = hasRemoteService;
    }
  }

  static KeyspaceMetadata ensureKeyspaceMetadata(CqlSession session, String keyspace) {
    KeyspaceMetadata keyspaceMetadata = getKeyspaceMetadata(session, keyspace);
    if (keyspaceMetadata == null) {
      throw new IllegalStateException(
        String.format(
          "Cannot read keyspace metadata for keyspace: %s and cluster: %s",
          keyspace, session.getMetadata().getClusterName()));
    }
    return keyspaceMetadata;
  }

  @Nullable static KeyspaceMetadata getKeyspaceMetadata(CqlSession session, String keyspace) {
    com.datastax.oss.driver.api.core.metadata.Metadata metadata = session.getMetadata();
    for (Node node : metadata.getNodes().values()) {
      Version version = node.getCassandraVersion();
      if (version == null) throw new RuntimeException("node had no version: " + node);
      if (Version.parse("3.11.3").compareTo(version) < 0) {
        throw new RuntimeException(String.format(
          "Host %s is running Cassandra %s, but minimum version is 3.11.3",
          node.getHostId(), node.getCassandraVersion()));
      }
    }
    return metadata.getKeyspace(keyspace).orElse(null);
  }

  static KeyspaceMetadata ensureExists(String keyspace, boolean searchEnabled, CqlSession session) {
    KeyspaceMetadata result = getKeyspaceMetadata(session, keyspace);
    if (result == null || !result.getTable(Schema.TABLE_SPAN).isPresent()) {
      LOG.info("Installing schema {} for keyspace {}", SCHEMA_RESOURCE, keyspace);
      applyCqlFile(keyspace, session, SCHEMA_RESOURCE);
      if (searchEnabled) {
        LOG.info("Installing indexes {} for keyspace {}", INDEX_RESOURCE, keyspace);
        applyCqlFile(keyspace, session, INDEX_RESOURCE);
      }
      // refresh metadata since we've installed the schema
      result = ensureKeyspaceMetadata(session, keyspace);
    }
    if (!hasUpgrade1_autocompleteTags(result)) {
      LOG.info("Upgrading schema {}", UPGRADE_1);
      applyCqlFile(keyspace, session, UPGRADE_1);
    }
    if (!hasUpgrade2_remoteService(result)) {
      LOG.info("Upgrading schema {}", UPGRADE_2);
      applyCqlFile(keyspace, session, UPGRADE_2);
    }
    return result;
  }

  static boolean hasUpgrade1_autocompleteTags(KeyspaceMetadata keyspaceMetadata) {
    return keyspaceMetadata.getTable(TABLE_AUTOCOMPLETE_TAGS).isPresent();
  }

  static boolean hasUpgrade2_remoteService(KeyspaceMetadata keyspaceMetadata) {
    return keyspaceMetadata.getTable(TABLE_SERVICE_REMOTE_SERVICES).isPresent();
  }

  static void applyCqlFile(String keyspace, CqlSession session, String resource) {
    for (String cmd : resourceToString(resource).split(";", 100)) {
      cmd = cmd.trim().replace(" " + DEFAULT_KEYSPACE, " " + keyspace);
      if (!cmd.isEmpty()) {
        session.execute(cmd);
      }
    }
  }
  //
  //
  //
  ////@UDT(name = "endpoint")
  //static final class EndpointUDT implements Serializable { // for Spark jobs
  //  static final long serialVersionUID = 0L;
  //
  //  String service;
  //  InetAddress ipv4;
  //  InetAddress ipv6;
  //  int port;
  //
  //  EndpointUDT() {
  //    this.service = null;
  //    this.ipv4 = null;
  //    this.ipv6 = null;
  //    this.port = 0;
  //  }
  //
  //  EndpointUDT(Endpoint endpoint) {
  //    this.service = endpoint.serviceName();
  //    this.ipv4 = endpoint.ipv4() == null ? null : InetAddresses.forString(endpoint.ipv4());
  //    this.ipv6 = endpoint.ipv6() == null ? null : InetAddresses.forString(endpoint.ipv6());
  //    this.port = endpoint.portAsInt();
  //  }
  //
  //  public String getService() {
  //    return service;
  //  }
  //
  //  public InetAddress getIpv4() {
  //    return ipv4;
  //  }
  //
  //  public InetAddress getIpv6() {
  //    return ipv6;
  //  }
  //
  //  public int getPort() {
  //    return port;
  //  }
  //
  //  public void setService(String service) {
  //    this.service = service;
  //  }
  //
  //  public void setIpv4(InetAddress ipv4) {
  //    this.ipv4 = ipv4;
  //  }
  //
  //  public void setIpv6(InetAddress ipv6) {
  //    this.ipv6 = ipv6;
  //  }
  //
  //  public void setPort(int port) {
  //    this.port = port;
  //  }
  //
  //  Endpoint toEndpoint() {
  //    Endpoint.Builder builder = Endpoint.newBuilder().serviceName(service).port(port);
  //    builder.parseIp(ipv4);
  //    builder.parseIp(ipv6);
  //    return builder.build();
  //  }
  //
  //  @Override public String toString() {
  //    return "EndpointUDT{"
  //      + "service=" + service + ", "
  //      + "ipv4=" + ipv4 + ", "
  //      + "ipv6=" + ipv6 + ", "
  //      + "port=" + port
  //      + "}";
  //  }
  //}
  //
  ////@UDT(name = "annotation")
  //static final class AnnotationUDT implements Serializable { // for Spark jobs
  //  static final long serialVersionUID = 0L;
  //
  //  long ts;
  //  String v;
  //
  //  AnnotationUDT() {
  //    this.ts = 0;
  //    this.v = null;
  //  }
  //
  //  AnnotationUDT(Annotation annotation) {
  //    this.ts = annotation.timestamp();
  //    this.v = annotation.value();
  //  }
  //
  //  public long getTs() {
  //    return ts;
  //  }
  //
  //  public String getV() {
  //    return v;
  //  }
  //
  //  public void setTs(long ts) {
  //    this.ts = ts;
  //  }
  //
  //  public void setV(String v) {
  //    this.v = v;
  //  }
  //
  //  Annotation toAnnotation() {
  //    return Annotation.create(ts, v);
  //  }
  //
  //  @Override public String toString() {
  //    return "AnnotationUDT{ts=" + ts + ", v=" + v + "}";
  //  }
  //}
}
