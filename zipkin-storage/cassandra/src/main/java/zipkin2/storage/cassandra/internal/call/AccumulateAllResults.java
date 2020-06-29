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
package zipkin2.storage.cassandra.internal.call;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import com.google.auto.value.AutoValue;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;
import zipkin2.Call;
import zipkin2.Call.FlatMapper;

public abstract class AccumulateAllResults<T> implements FlatMapper<AsyncResultSet, T> {
  protected abstract Supplier<T> supplier();

  protected abstract BiConsumer<Row, T> accumulator();

  /** Customizes the aggregated result. For example, summarizing or making immutable. */
  protected Function<T, T> finisher() {
    return Function.identity();
  }

  @Override public Call<T> map(AsyncResultSet rs) {
    return new AutoValue_AccumulateAllResults_AccumulateNextResults<>(
      supplier().get(),
      accumulator(),
      finisher()
    ).map(rs);
  }

  @AutoValue
  static abstract class FetchMoreResults extends ResultSetFutureCall<AsyncResultSet> {
    static FetchMoreResults create(AsyncResultSet resultSet) {
      return new AutoValue_AccumulateAllResults_FetchMoreResults(resultSet);
    }

    abstract AsyncResultSet resultSet();

    @Override protected CompletableFuture<AsyncResultSet> newFuture() {
      return resultSet().fetchNextPage().toCompletableFuture();
    }

    @Override public AsyncResultSet map(AsyncResultSet input) {
      return input;
    }

    @Override public Call<AsyncResultSet> clone() {
      throw new UnsupportedOperationException();
    }
  }

  @AutoValue
  static abstract class AccumulateNextResults<T> implements FlatMapper<AsyncResultSet, T> {
    abstract T pendingResults();

    abstract BiConsumer<Row, T> accumulator();

    abstract Function<T, T> finisher();

    /** Iterates through the rows in each page, flatmapping on more results until exhausted */
    @Override public Call<T> map(AsyncResultSet rs) {
      while (rs.remaining() > 0) {
        accumulator().accept(rs.one(), pendingResults());
      }
      // Return collected results if there are no more pages
      return rs.getExecutionInfo().getPagingState() == null && !rs.hasMorePages()
        ? Call.create(finisher().apply(pendingResults()))
        : FetchMoreResults.create(rs).flatMap(this);
    }
  }
}
