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

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.querybuilder.QueryBuilder;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import zipkin2.Call;
import zipkin2.storage.cassandra.internal.call.DistinctSortedStrings;
import zipkin2.storage.cassandra.internal.call.ResultSetFutureCall;

import static zipkin2.storage.cassandra.Schema.TABLE_SERVICE_REMOTE_SERVICES;

final class SelectRemoteServiceNames extends ResultSetFutureCall<ResultSet> {

  static class Factory {
    final CqlSession session;
    final PreparedStatement preparedStatement;
    final DistinctSortedStrings remoteServices = new DistinctSortedStrings("remote_service");

    Factory(CqlSession session) {
      this.session = session;
      this.preparedStatement =
        session.prepare(QueryBuilder.selectFrom(TABLE_SERVICE_REMOTE_SERVICES)
          .column("remote_service")
          .whereColumn("service").isEqualTo(QueryBuilder.bindMarker("service"))
          .limit(QueryBuilder.bindMarker("limit_")).build());
    }

    Call<List<String>> create(String serviceName) {
      if (serviceName == null || serviceName.isEmpty()) return Call.emptyList();
      String service = serviceName.toLowerCase();
      return new SelectRemoteServiceNames(this, service).flatMap(remoteServices);
    }
  }

  final Factory factory;
  final String service;

  SelectRemoteServiceNames(Factory factory, String service) {
    this.factory = factory;
    this.service = service;
  }

  @Override
  protected CompletableFuture<AsyncResultSet> newFuture() {
    return factory.session.executeAsync(factory.preparedStatement
      .bind()
      .setString("service", service)
      .setInt("limit_", 1000)); // no one is ever going to browse so many service names
  }

  @Override public ResultSet map(ResultSet input) {
    return input;
  }

  @Override
  public String toString() {
    return "SelectSpanNames{service=" + service + "}";
  }

  @Override
  public SelectRemoteServiceNames clone() {
    return new SelectRemoteServiceNames(factory, service);
  }
}
