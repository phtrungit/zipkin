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
package zipkin2.storage.cassandra.internal.call;

import com.datastax.oss.driver.api.core.cql.AsyncResultSet;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.BiConsumer;
import zipkin2.Call;
import zipkin2.Callback;


// some copy/pasting is ok here as debugging is obscured when the type hierarchy gets deep.
public abstract class ResultSetFutureCall<V> extends Call.Base<V>
  implements Call.Mapper<AsyncResultSet, V> {
  /** Defers I/O until {@link #enqueue(Callback)} or {@link #execute()} are called. */
  protected abstract CompletableFuture<AsyncResultSet> newFuture();

  volatile CompletableFuture<AsyncResultSet> future;

  @Override
  protected V doExecute() {
    return map(getUninterruptibly(future = newFuture()));
  }

  @Override
  protected void doEnqueue(Callback<V> callback) {
    class CallbackConsumer implements BiConsumer<AsyncResultSet, Throwable> {
      @Override public void accept(AsyncResultSet input, Throwable error) {
        if (error != null) {
          callback.onError(error);
          return;
        }
        try {
          callback.onSuccess(map(input));
        } catch (Throwable t) {
          propagateIfFatal(t);
          callback.onError(t);
        }
      }
    }
    try {
      (future = newFuture()).whenComplete(new CallbackConsumer());
    } catch (Throwable t) {
      propagateIfFatal(t);
      callback.onError(t);
      throw t;
    }
  }

  @Override
  protected void doCancel() {
    CompletableFuture<AsyncResultSet> maybeFuture = future;
    if (maybeFuture != null) maybeFuture.cancel(true);
  }

  @Override
  protected final boolean doIsCanceled() {
    CompletableFuture<AsyncResultSet> maybeFuture = future;
    return maybeFuture != null && maybeFuture.isCancelled();
  }

  /**
   * Sets {@link zipkin2.storage.StorageComponent#isOverCapacity(java.lang.Throwable)}
   */
  public static boolean isOverCapacity(Throwable e) {
    return e instanceof QueryConsistencyException ||
      e instanceof BusyConnectionException ||
      e instanceof BusyPoolException;
  }

  static ResultSet getUninterruptibly(CompletableFuture<ResultSet> future) {
    if (future instanceof ResultSetFuture) {
      return ((ResultSetFuture) future).getUninterruptibly();
    }
    try { // emulate ResultSetFuture.getUninterruptibly
      return Uninterruptibles.getUninterruptibly(future);
    } catch (ExecutionException e) {
      Throwable cause = e.getCause();
      if (cause instanceof Error) throw ((Error) cause);
      if (cause instanceof DriverException) throw ((DriverException) cause).copy();
      throw new DriverInternalError("Unexpected exception thrown", cause);
    }
  }
}
