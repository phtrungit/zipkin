/*
 * Copyright 2015-2020 The OpenZipkin Authors
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
package zipkin2.server.internal.elasticsearch;

import com.linecorp.armeria.client.endpoint.EmptyEndpointGroupException;
import com.linecorp.armeria.server.ServerBuilder;
import com.linecorp.armeria.server.healthcheck.HealthCheckService;
import com.linecorp.armeria.server.healthcheck.SettableHealthChecker;
import com.linecorp.armeria.testing.junit4.server.ServerRule;
import java.util.concurrent.TimeUnit;
import javax.net.ssl.SSLHandshakeException;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.springframework.boot.test.util.TestPropertyValues;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import zipkin2.CheckResult;
import zipkin2.elasticsearch.ElasticsearchStorage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static zipkin2.server.internal.elasticsearch.TestResponses.GREEN_RESPONSE;
import static zipkin2.server.internal.elasticsearch.TestResponses.VERSION_RESPONSE;

/**
 * These tests focus on http client health checks not currently in zipkin-storage-elasticsearch.
 */
public class ITElasticsearchHealthCheck {
  static final SettableHealthChecker server1Health = new SettableHealthChecker(true);

  @ClassRule public static ServerRule server1 = new ServerRule() {
    @Override protected void configure(ServerBuilder sb) {
      sb.service("/", (ctx, req) -> VERSION_RESPONSE.toHttpResponse());
      sb.service("/_cluster/health", HealthCheckService.of(server1Health));
      sb.serviceUnder("/_cluster/health/", (ctx, req) -> GREEN_RESPONSE.toHttpResponse());
    }
  };

  static final SettableHealthChecker server2Health = new SettableHealthChecker(true);

  @ClassRule public static ServerRule server2 = new ServerRule() {
    @Override protected void configure(ServerBuilder sb) {
      sb.service("/", (ctx, req) -> VERSION_RESPONSE.toHttpResponse());
      sb.service("/_cluster/health", HealthCheckService.of(server2Health));
      sb.serviceUnder("/_cluster/health/", (ctx, req) -> GREEN_RESPONSE.toHttpResponse());
    }
  };

  AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();

  @Before public void setUp() {
    server1Health.setHealthy(true);
    server2Health.setHealthy(true);

    initWithHosts("127.0.0.1:" + server1.httpPort() + ",127.0.0.1:" + server2.httpPort());
  }

  private void initWithHosts(String hosts) {
    TestPropertyValues.of(
      "spring.config.name=zipkin-server",
      "zipkin.storage.type=elasticsearch",
      "zipkin.storage.elasticsearch.ensure-templates=false",
      "zipkin.storage.elasticsearch.timeout=200",
      "zipkin.storage.elasticsearch.health-check.enabled=true",
      // uncomment (and also change log4j2.properties) to see health-checks requests in the console
      "zipkin.storage.elasticsearch.health-check.http-logging=headers",
      "zipkin.storage.elasticsearch.health-check.interval=100ms",
      "zipkin.storage.elasticsearch.hosts=" + hosts)
      .applyTo(context);
    Access.registerElasticsearch(context);
    context.refresh();
  }

  @Test public void allHealthy() {
    try (ElasticsearchStorage storage = context.getBean(ElasticsearchStorage.class)) {
      CheckResult result = storage.check();
      assertThat(result.ok()).isTrue();
    }
  }

  @Test public void oneHealthy() {
    server1Health.setHealthy(false);

    try (ElasticsearchStorage storage = context.getBean(ElasticsearchStorage.class)) {
      CheckResult result = storage.check();
      assertThat(result.ok()).isTrue();
    }
  }

  @Test public void wrongScheme() {
    context.close();
    context = new AnnotationConfigApplicationContext();
    initWithHosts("https://localhost:" + server1.httpPort());

    try (ElasticsearchStorage storage = context.getBean(ElasticsearchStorage.class)) {
      CheckResult result = storage.check();
      assertThat(result.ok()).isFalse();
      // Test this is not wrapped in a rejection exception, as health check is not throttled
      assertThat(result.error()).isInstanceOf(SSLHandshakeException.class);
    }
  }

  @Test public void noneHealthy() {
    server1Health.setHealthy(false);
    server2Health.setHealthy(false);

    try (ElasticsearchStorage storage = context.getBean(ElasticsearchStorage.class)) {
      CheckResult result = storage.check();
      assertThat(result.ok()).isFalse();
      assertThat(result.error())
        .isInstanceOf(EmptyEndpointGroupException.class);
    }
  }

  // TODO: FIXME
  @Test public void healthyThenNotHealthyThenHealthy() {
    try (ElasticsearchStorage storage = context.getBean(ElasticsearchStorage.class)) {
      CheckResult result = storage.check();
      assertThat(result.ok()).isTrue();

      server1Health.setHealthy(false);
      server2Health.setHealthy(false);

      // Health check interval is 100ms
      await().timeout(300, TimeUnit.MILLISECONDS).untilAsserted(() ->
        assertThat(storage.check().ok()).isFalse());

      server1Health.setHealthy(true);

      // Health check interval is 100ms
      await().timeout(300, TimeUnit.MILLISECONDS).untilAsserted(() ->
        assertThat(storage.check().ok()).isTrue());
    }
  }

  // TODO: FIXME
  @Test public void notHealthyThenHealthyThenNotHealthy() {
    server1Health.setHealthy(false);
    server2Health.setHealthy(false);

    try (ElasticsearchStorage storage = context.getBean(ElasticsearchStorage.class)) {
      CheckResult result = storage.check();
      assertThat(result.ok()).isFalse();

      server2Health.setHealthy(true);

      // Health check interval is 100ms
      await().timeout(300, TimeUnit.MILLISECONDS).untilAsserted(() ->
        assertThat(storage.check().ok()).isTrue());

      server2Health.setHealthy(false);

      // Health check interval is 100ms
      await().timeout(300, TimeUnit.MILLISECONDS).untilAsserted(() ->
        assertThat(storage.check().ok()).isFalse());
    }
  }

  @Test public void healthCheckDisabled() {
    AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext();

    TestPropertyValues.of(
      "spring.config.name=zipkin-server",
      "zipkin.storage.type:elasticsearch",
      "zipkin.storage.elasticsearch.ensure-templates=false",
      "zipkin.storage.elasticsearch.timeout=200",
      "zipkin.storage.elasticsearch.health-check.enabled=false",
      "zipkin.storage.elasticsearch.health-check.interval=100ms",
      "zipkin.storage.elasticsearch.hosts=127.0.0.1:" +
        server1.httpPort() + ",127.0.0.1:" + server2.httpPort())
      .applyTo(context);
    Access.registerElasticsearch(context);
    context.refresh();

    server1Health.setHealthy(false);
    server2Health.setHealthy(false);

    try (ElasticsearchStorage storage = context.getBean(ElasticsearchStorage.class)) {
      // Even though cluster health is false, we ignore that and continue to check index health,
      // which is correctly returned by our mock server.
      CheckResult result = storage.check();
      assertThat(result.ok()).isTrue();
    }
  }
}
