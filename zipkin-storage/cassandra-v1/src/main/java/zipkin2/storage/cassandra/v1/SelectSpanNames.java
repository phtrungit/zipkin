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
package zipkin2.storage.cassandra.v1;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.ResultSetFuture;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.cql.querybuilder.QueryBuilder;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import zipkin2.Call;
import zipkin2.storage.cassandra.internal.call.DistinctSortedStrings;
import zipkin2.storage.cassandra.internal.call.ResultSetFutureCall;

import static com.google.common.base.Preconditions.checkNotNull;

final class SelectSpanNames extends ResultSetFutureCall<AsyncResultSet> {

  static class Factory {
    final CqlSession session;
    final PreparedStatement preparedStatement;
    final DistinctSortedStrings spanNames = new DistinctSortedStrings("span_name");

    Factory(CqlSession session) {
      this.session = session;
      this.preparedStatement = session.prepare(QueryBuilder.select("span_name")
        .from(Tables.SPAN_NAMES)
        .where(QueryBuilder.eq("service_name", QueryBuilder.bindMarker("service_name")))
        .and(QueryBuilder.eq("bucket", 0))
        .limit(QueryBuilder.bindMarker("limit_")));
    }

    Call<List<String>> create(String serviceName) {
      if (serviceName == null || serviceName.isEmpty()) return Call.emptyList();
      String service = checkNotNull(serviceName, "serviceName").toLowerCase();
      return new SelectSpanNames(this, service).flatMap(spanNames);
    }
  }

  final Factory factory;
  final String service_name;

  SelectSpanNames(Factory factory, String service_name) {
    this.factory = factory;
    this.service_name = service_name;
  }

  @Override protected CompletableFuture<AsyncResultSet> newFuture() {
    return factory.session.executeAsync(factory.preparedStatement.bind()
      .setString("service_name", service_name)
      .setInt("limit_", 10000)).toCompletableFuture();
  }

  @Override public AsyncResultSet map(AsyncResultSet input) {
    return input;
  }

  @Override public String toString() {
    return "SelectSpanNames{service_name=" + service_name + "}";
  }

  @Override public SelectSpanNames clone() {
    return new SelectSpanNames(factory, service_name);
  }
}
