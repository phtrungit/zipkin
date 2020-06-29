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
import com.datastax.oss.driver.api.core.cql.querybuilder.Insert;
import com.datastax.oss.driver.api.core.cql.querybuilder.QueryBuilder;
import java.util.Map;
import java.util.concurrent.CompletionStage;
import zipkin2.Call;
import zipkin2.storage.cassandra.internal.call.DeduplicatingVoidCallFactory;
import zipkin2.storage.cassandra.internal.call.ResultSetFutureCall;

import static zipkin2.storage.cassandra.v1.Tables.AUTOCOMPLETE_TAGS;

final class InsertAutocompleteValue extends ResultSetFutureCall<Void> {

  static class Factory extends DeduplicatingVoidCallFactory<Map.Entry<String, String>> {
    final CqlSession session;
    final PreparedStatement preparedStatement;

    Factory(CassandraStorage storage, int indexTtl) {
      super(storage.autocompleteTtl, storage.autocompleteCardinality);
      session = storage.session();
      Insert insertQuery = QueryBuilder.insertInto(AUTOCOMPLETE_TAGS)
        .value("key", QueryBuilder.bindMarker("key"))
        .value("value", QueryBuilder.bindMarker("value"));
      if (indexTtl > 0) insertQuery.using(QueryBuilder.ttl(indexTtl));
      preparedStatement = session.prepare(insertQuery);
    }

    @Override protected InsertAutocompleteValue newCall(Map.Entry<String, String> input) {
      return new InsertAutocompleteValue(this, input);
    }
  }

  final Factory factory;
  final Map.Entry<String, String> input;

  InsertAutocompleteValue(Factory factory, Map.Entry<String, String> input) {
    this.factory = factory;
    this.input = input;
  }

  @Override protected CompletionStage<AsyncResultSet> newFuture() {
    return factory.session.executeAsync(factory.preparedStatement.bind()
      .setString("key", input.getKey())
      .setString("value", input.getValue()));
  }

  @Override public Void map(ResultSet input) {
    return null;
  }

  @Override public String toString() {
    return "InsertAutocompleteValue(" + input + ")";
  }

  @Override public Call<Void> clone() {
    return new InsertAutocompleteValue(factory, input);
  }
}
