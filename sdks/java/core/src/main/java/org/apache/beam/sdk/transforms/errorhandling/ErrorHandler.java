/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.transforms.errorhandling;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.apache.beam.sdk.annotations.Internal;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.POutput;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An Error Handler is a utility object used for plumbing error PCollections to a configured sink
 * Error Handlers must be closed before a pipeline is run to properly pipe error collections to the
 * sink, and the pipeline will be rejected if any handlers aren't closed.
 *
 * @param <ErrorT> The type of the error object. This will usually be a {@link BadRecord}, but can
 *     be any type
 * @param <OutputT> The return type of the sink PTransform.
 *     <p>Usage of Error Handlers:
 *     <p>Simple usage with one DLQ
 *     <pre>{@code
 * PCollection<?> records = ...;
 * try (ErrorHandler<E,T> errorHandler = pipeline.registerErrorHandler(SomeSink.write())) {
 *  PCollection<?> results = records.apply(SomeIO.write().withDeadLetterQueue(errorHandler));
 * }
 * results.apply(SomeOtherTransform);
 * }</pre>
 *     Usage with multiple DLQ stages
 *     <pre>{@code
 * PCollection<?> records = ...;
 * try (ErrorHandler<E,T> errorHandler = pipeline.registerErrorHandler(SomeSink.write())) {
 *  PCollection<?> results = records.apply(SomeIO.write().withDeadLetterQueue(errorHandler))
 *                        .apply(OtherTransform.builder().withDeadLetterQueue(errorHandler));
 * }
 * results.apply(SomeOtherTransform);
 * }</pre>
 */
public interface ErrorHandler<ErrorT, OutputT extends POutput> extends AutoCloseable {

  void addErrorCollection(PCollection<ErrorT> errorCollection);

  boolean isClosed();

  @Nullable
  OutputT getOutput();

  class PTransformErrorHandler<ErrorT, OutputT extends POutput>
      implements ErrorHandler<ErrorT, OutputT> {

    private static final Logger LOG = LoggerFactory.getLogger(PTransformErrorHandler.class);
    private final PTransform<PCollection<ErrorT>, OutputT> sinkTransform;

    private final List<PCollection<ErrorT>> errorCollections = new ArrayList<>();

    private @Nullable OutputT sinkOutput = null;

    private boolean closed = false;

    /**
     * Constructs a new ErrorHandler, but should not be called directly. Instead, call
     * pipeline.registerErrorHandler to ensure safe pipeline construction
     */
    @Internal
    public PTransformErrorHandler(PTransform<PCollection<ErrorT>, OutputT> sinkTransform) {
      this.sinkTransform = sinkTransform;
    }

    @Override
    public void addErrorCollection(PCollection<ErrorT> errorCollection) {
      errorCollections.add(errorCollection);
    }

    @Override
    public boolean isClosed() {
      return closed;
    }

    @Override
    public @Nullable OutputT getOutput() {
      if (!this.isClosed()) {
        throw new IllegalStateException(
            "ErrorHandler must be finalized before the output can be returned");
      }
      return sinkOutput;
    }

    @Override
    public void close() {
      closed = true;
      if (errorCollections.isEmpty()) {
        LOG.warn("Empty list of error pcollections passed to ErrorHandler.");
        return;
      }
      LOG.debug(
          "{} error collections are being sent to {}",
          errorCollections.size(),
          sinkTransform.getName());
      String sinkTransformName = sinkTransform.getName();
      sinkOutput =
          PCollectionList.of(errorCollections)
              .apply(Flatten.pCollections())
              .apply(
                  "Record Error Metrics to " + sinkTransformName,
                  new WriteErrorMetrics<ErrorT>(sinkTransformName))
              .apply(
                  "Write to error Sink",
                  sinkTransform.addAnnotation(
                      "FeatureMetric", "ErrorHandler".getBytes(StandardCharsets.UTF_8)));
    }

    public static class WriteErrorMetrics<ErrorT>
        extends PTransform<PCollection<ErrorT>, PCollection<ErrorT>> {

      private final Counter errorCounter;

      public WriteErrorMetrics(String sinkTransformName) {
        errorCounter = Metrics.counter("ErrorMetrics", sinkTransformName + "-input");
      }

      @Override
      public PCollection<ErrorT> expand(PCollection<ErrorT> input) {
        return input.apply(ParDo.of(new CountErrors<ErrorT>(errorCounter)));
      }

      public static class CountErrors<ErrorT> extends DoFn<ErrorT, ErrorT> {

        private final Counter errorCounter;

        public CountErrors(Counter errorCounter) {
          this.errorCounter = errorCounter;
        }

        @ProcessElement
        public void processElement(@Element ErrorT error, OutputReceiver<ErrorT> receiver) {
          errorCounter.inc();
          receiver.output(error);
        }
      }
    }
  }

  /**
   * A default, placeholder error handler that exists to allow usage of .addErrorCollection()
   * without effects. This enables more simple codepaths without checking for whether the user
   * configured an error handler or not.
   */
  @Internal
  class NoOpErrorHandler<ErrorT, OutputT extends POutput> implements ErrorHandler<ErrorT, OutputT> {

    @Override
    public void addErrorCollection(PCollection<ErrorT> errorCollection) {}

    @Override
    public boolean isClosed() {
      throw new IllegalArgumentException(
          "No Op handler should not be closed. This implies this IO is misconfigured.");
    }

    @Override
    public @Nullable OutputT getOutput() {
      throw new IllegalArgumentException(
          "No Op handler has no output. This implies this IO is misconfigured.");
    }

    @Override
    public void close() {
      throw new IllegalArgumentException(
          "No Op handler should not be closed. This implies this IO is misconfigured.");
    }
  }
}
