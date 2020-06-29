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

import static zipkin2.storage.cassandra.Schema.TABLE_AUTOCOMPLETE_TAGS;

final class SelectAutocompleteValues extends ResultSetFutureCall<ResultSet> {

  static class Factory {
    final CqlSession session;
    final PreparedStatement preparedStatement;
    final DistinctSortedStrings values = new DistinctSortedStrings("value");

    Factory(CqlSession session) {
      this.session = session;
      this.preparedStatement = session.prepare(
        QueryBuilder.selectFrom(TABLE_AUTOCOMPLETE_TAGS)
          .column("value")
          .whereColumn("key").isEqualTo(QueryBuilder.bindMarker("key"))
          .limit(QueryBuilder.bindMarker("limit_")).build());
    }

    Call<List<String>> create(String key) {
      return new SelectAutocompleteValues(this, key).flatMap(values);
    }
  }

  final SelectAutocompleteValues.Factory factory;
  final String key;

  SelectAutocompleteValues(SelectAutocompleteValues.Factory factory, String key) {
    this.factory = factory;
    this.key = key;
  }

  @Override protected CompletableFuture<AsyncResultSet> newFuture() {
    return factory.session.executeAsync(factory.preparedStatement.bind()
      .setString("key", key)
      .setInt("limit_", 10000));
  }

  @Override public ResultSet map(ResultSet input) {
    return input;
  }

  @Override public Call<ResultSet> clone() {
    return new SelectAutocompleteValues(factory, key);
  }
}
