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
package org.apache.beam.sdk.io.gcp.bigquery;

import static org.apache.beam.sdk.transforms.errorhandling.BadRecordRouter.BAD_RECORD_TAG;

import com.google.api.services.bigquery.model.TableRow;
import com.google.cloud.bigquery.storage.v1.AppendRowsRequest;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.ThreadLocalRandom;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.schemas.NoSuchSchemaException;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.GroupIntoBatches;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.errorhandling.BadRecord;
import org.apache.beam.sdk.transforms.errorhandling.BadRecordRouter;
import org.apache.beam.sdk.transforms.errorhandling.BadRecordRouter.ThrowingBadRecordRouter;
import org.apache.beam.sdk.transforms.errorhandling.ErrorHandler;
import org.apache.beam.sdk.transforms.windowing.GlobalWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.beam.sdk.util.ShardedKey;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.TupleTag;
import org.joda.time.Duration;

/** This {@link PTransform} manages loads into BigQuery using the Storage API. */
public class StorageApiLoads<DestinationT, ElementT>
    extends PTransform<PCollection<KV<DestinationT, ElementT>>, WriteResult> {
  final TupleTag<KV<DestinationT, KV<ElementT, StorageApiWritePayload>>>
      successfulConvertedRowsTag = new TupleTag<>("successfulRows");

  final TupleTag<BigQueryStorageApiInsertError> failedRowsTag = new TupleTag<>("failedRows");

  @Nullable TupleTag<TableRow> successfulWrittenRowsTag;
  private final Coder<DestinationT> destinationCoder;
  private final Coder<ElementT> elementCoder;
  private final StorageApiDynamicDestinations<ElementT, DestinationT> dynamicDestinations;

  private final @Nullable SerializableFunction<ElementT, RowMutationInformation> rowUpdateFn;
  private final @Nullable SerializableFunction<ElementT, TableRow> formatRecordOnFailureFunction;
  private final CreateDisposition createDisposition;
  private final String kmsKey;
  private final Duration triggeringFrequency;
  private final BigQueryServices bqServices;
  private final int numShards;
  private final boolean allowInconsistentWrites;
  private final boolean allowAutosharding;
  private final boolean autoUpdateSchema;
  private final boolean ignoreUnknownValues;
  private final boolean usesCdc;

  private final AppendRowsRequest.MissingValueInterpretation defaultMissingValueInterpretation;

  private final BadRecordRouter badRecordRouter;

  private final ErrorHandler<BadRecord, ?> badRecordErrorHandler;

  public StorageApiLoads(
      Coder<DestinationT> destinationCoder,
      Coder<ElementT> elementCoder,
      StorageApiDynamicDestinations<ElementT, DestinationT> dynamicDestinations,
      @Nullable SerializableFunction<ElementT, RowMutationInformation> rowUpdateFn,
      @Nullable SerializableFunction<ElementT, TableRow> formatRecordOnFailureFunction,
      CreateDisposition createDisposition,
      String kmsKey,
      Duration triggeringFrequency,
      BigQueryServices bqServices,
      int numShards,
      boolean allowInconsistentWrites,
      boolean allowAutosharding,
      boolean autoUpdateSchema,
      boolean ignoreUnknownValues,
      boolean propagateSuccessfulStorageApiWrites,
      boolean usesCdc,
      AppendRowsRequest.MissingValueInterpretation defaultMissingValueInterpretation,
      BadRecordRouter badRecordRouter,
      ErrorHandler<BadRecord, ?> badRecordErrorHandler) {
    this.destinationCoder = destinationCoder;
    this.elementCoder = elementCoder;
    this.dynamicDestinations = dynamicDestinations;
    this.rowUpdateFn = rowUpdateFn;
    this.formatRecordOnFailureFunction = formatRecordOnFailureFunction;
    this.createDisposition = createDisposition;
    this.kmsKey = kmsKey;
    this.triggeringFrequency = triggeringFrequency;
    this.bqServices = bqServices;
    this.numShards = numShards;
    this.allowInconsistentWrites = allowInconsistentWrites;
    this.allowAutosharding = allowAutosharding;
    this.autoUpdateSchema = autoUpdateSchema;
    this.ignoreUnknownValues = ignoreUnknownValues;
    if (propagateSuccessfulStorageApiWrites) {
      this.successfulWrittenRowsTag = new TupleTag<>("successfulPublishedRowsTag");
    }
    this.usesCdc = usesCdc;
    this.defaultMissingValueInterpretation = defaultMissingValueInterpretation;
    this.badRecordRouter = badRecordRouter;
    this.badRecordErrorHandler = badRecordErrorHandler;
  }

  public TupleTag<BigQueryStorageApiInsertError> getFailedRowsTag() {
    return failedRowsTag;
  }

  public boolean usesErrorHandler() {
    return !(badRecordRouter instanceof ThrowingBadRecordRouter);
  }

  @Override
  public WriteResult expand(PCollection<KV<DestinationT, ElementT>> input) {
    Coder<StorageApiWritePayload> payloadCoder;
    try {
      payloadCoder =
          input.getPipeline().getSchemaRegistry().getSchemaCoder(StorageApiWritePayload.class);
    } catch (NoSuchSchemaException e) {
      throw new RuntimeException(e);
    }
    Coder<KV<DestinationT, KV<ElementT, StorageApiWritePayload>>> successCoder =
        KvCoder.of(destinationCoder, KvCoder.of(elementCoder, payloadCoder));
    if (allowInconsistentWrites) {
      return expandInconsistent(input, successCoder);
    } else {
      return triggeringFrequency != null
          ? expandTriggered(input, successCoder, payloadCoder)
          : expandUntriggered(input, successCoder);
    }
  }

  public WriteResult expandInconsistent(
      PCollection<KV<DestinationT, ElementT>> input,
      Coder<KV<DestinationT, KV<ElementT, StorageApiWritePayload>>> successCoder) {
    PCollection<KV<DestinationT, ElementT>> inputInGlobalWindow =
        input.apply("rewindowIntoGlobal", Window.into(new GlobalWindows()));

    PCollectionTuple convertMessagesResult =
        inputInGlobalWindow.apply(
            "Convert",
            new StorageApiConvertMessages<>(
                dynamicDestinations,
                bqServices,
                failedRowsTag,
                successfulConvertedRowsTag,
                BigQueryStorageApiInsertErrorCoder.of(),
                successCoder,
                rowUpdateFn,
                badRecordRouter,
                formatRecordOnFailureFunction));
    PCollectionTuple writeRecordsResult =
        convertMessagesResult
            .get(successfulConvertedRowsTag)
            .apply(
                "StorageApiWriteInconsistent",
                new StorageApiWriteRecordsInconsistent<>(
                    dynamicDestinations,
                    bqServices,
                    failedRowsTag,
                    successfulWrittenRowsTag,
                    BigQueryStorageApiInsertErrorCoder.of(),
                    TableRowJsonCoder.of(),
                    autoUpdateSchema,
                    ignoreUnknownValues,
                    createDisposition,
                    kmsKey,
                    usesCdc,
                    defaultMissingValueInterpretation,
                    formatRecordOnFailureFunction));

    PCollection<BigQueryStorageApiInsertError> insertErrors =
        PCollectionList.of(convertMessagesResult.get(failedRowsTag))
            .and(writeRecordsResult.get(failedRowsTag))
            .apply("flattenErrors", Flatten.pCollections());
    @Nullable PCollection<TableRow> successfulWrittenRows = null;
    if (successfulWrittenRowsTag != null) {
      successfulWrittenRows = writeRecordsResult.get(successfulWrittenRowsTag);
    }

    addErrorCollections(convertMessagesResult, writeRecordsResult);

    return WriteResult.in(
        input.getPipeline(),
        null,
        null,
        null,
        null,
        null,
        failedRowsTag,
        insertErrors,
        successfulWrittenRowsTag,
        successfulWrittenRows);
  }

  public WriteResult expandTriggered(
      PCollection<KV<DestinationT, ElementT>> input,
      Coder<KV<DestinationT, KV<ElementT, StorageApiWritePayload>>> successCoder,
      Coder<StorageApiWritePayload> payloadCoder) {
    // Handle triggered, low-latency loads into BigQuery.
    PCollection<KV<DestinationT, ElementT>> inputInGlobalWindow =
        input.apply("rewindowIntoGlobal", Window.into(new GlobalWindows()));
    PCollectionTuple convertMessagesResult =
        inputInGlobalWindow.apply(
            "Convert",
            new StorageApiConvertMessages<>(
                dynamicDestinations,
                bqServices,
                failedRowsTag,
                successfulConvertedRowsTag,
                BigQueryStorageApiInsertErrorCoder.of(),
                successCoder,
                rowUpdateFn,
                badRecordRouter,
                formatRecordOnFailureFunction));

    PCollection<KV<ShardedKey<DestinationT>, Iterable<KV<ElementT, StorageApiWritePayload>>>>
        groupedRecords;

    int maxAppendBytes =
        input
            .getPipeline()
            .getOptions()
            .as(BigQueryOptions.class)
            .getStorageApiAppendThresholdBytes();
    if (this.allowAutosharding) {
      groupedRecords =
          convertMessagesResult
              .get(successfulConvertedRowsTag)
              .apply(
                  "GroupIntoBatches",
                  GroupIntoBatches.<DestinationT, KV<ElementT, StorageApiWritePayload>>ofByteSize(
                          maxAppendBytes,
                          (KV<ElementT, StorageApiWritePayload> e) ->
                              (long) e.getValue().getPayload().length)
                      .withMaxBufferingDuration(triggeringFrequency)
                      .withShardedKey());

    } else {
      PCollection<KV<ShardedKey<DestinationT>, KV<ElementT, StorageApiWritePayload>>>
          shardedRecords =
              createShardedKeyValuePairs(convertMessagesResult)
                  .setCoder(
                      KvCoder.of(
                          ShardedKey.Coder.of(destinationCoder),
                          KvCoder.of(elementCoder, payloadCoder)));
      groupedRecords =
          shardedRecords.apply(
              "GroupIntoBatches",
              GroupIntoBatches
                  .<ShardedKey<DestinationT>, KV<ElementT, StorageApiWritePayload>>ofByteSize(
                      maxAppendBytes,
                      (KV<ElementT, StorageApiWritePayload> e) ->
                          (long) e.getValue().getPayload().length)
                  .withMaxBufferingDuration(triggeringFrequency));
    }
    PCollectionTuple writeRecordsResult =
        groupedRecords.apply(
            "StorageApiWriteSharded",
            new StorageApiWritesShardedRecords<>(
                dynamicDestinations,
                createDisposition,
                kmsKey,
                bqServices,
                destinationCoder,
                BigQueryStorageApiInsertErrorCoder.of(),
                TableRowJsonCoder.of(),
                failedRowsTag,
                successfulWrittenRowsTag,
                autoUpdateSchema,
                ignoreUnknownValues,
                defaultMissingValueInterpretation,
                formatRecordOnFailureFunction));

    PCollection<BigQueryStorageApiInsertError> insertErrors =
        PCollectionList.of(convertMessagesResult.get(failedRowsTag))
            .and(writeRecordsResult.get(failedRowsTag))
            .apply("flattenErrors", Flatten.pCollections());

    @Nullable PCollection<TableRow> successfulWrittenRows = null;
    if (successfulWrittenRowsTag != null) {
      successfulWrittenRows = writeRecordsResult.get(successfulWrittenRowsTag);
    }

    addErrorCollections(convertMessagesResult, writeRecordsResult);

    return WriteResult.in(
        input.getPipeline(),
        null,
        null,
        null,
        null,
        null,
        failedRowsTag,
        insertErrors,
        successfulWrittenRowsTag,
        successfulWrittenRows);
  }

  private PCollection<KV<ShardedKey<DestinationT>, KV<ElementT, StorageApiWritePayload>>>
      createShardedKeyValuePairs(PCollectionTuple pCollection) {
    return pCollection
        .get(successfulConvertedRowsTag)
        .apply(
            "AddShard",
            ParDo.of(
                new DoFn<
                    KV<DestinationT, KV<ElementT, StorageApiWritePayload>>,
                    KV<ShardedKey<DestinationT>, KV<ElementT, StorageApiWritePayload>>>() {
                  int shardNumber;

                  @Setup
                  public void setup() {
                    shardNumber = ThreadLocalRandom.current().nextInt(numShards);
                  }

                  @ProcessElement
                  public void processElement(
                      @Element KV<DestinationT, KV<ElementT, StorageApiWritePayload>> element,
                      OutputReceiver<
                              KV<ShardedKey<DestinationT>, KV<ElementT, StorageApiWritePayload>>>
                          o) {
                    DestinationT destination = element.getKey();
                    ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
                    buffer.putInt(++shardNumber % numShards);
                    o.output(KV.of(ShardedKey.of(destination, buffer.array()), element.getValue()));
                  }
                }));
  }

  public WriteResult expandUntriggered(
      PCollection<KV<DestinationT, ElementT>> input,
      Coder<KV<DestinationT, KV<ElementT, StorageApiWritePayload>>> successCoder) {
    PCollection<KV<DestinationT, ElementT>> inputInGlobalWindow =
        input.apply(
            "rewindowIntoGlobal", Window.<KV<DestinationT, ElementT>>into(new GlobalWindows()));
    PCollectionTuple convertMessagesResult =
        inputInGlobalWindow.apply(
            "Convert",
            new StorageApiConvertMessages<>(
                dynamicDestinations,
                bqServices,
                failedRowsTag,
                successfulConvertedRowsTag,
                BigQueryStorageApiInsertErrorCoder.of(),
                successCoder,
                rowUpdateFn,
                badRecordRouter,
                formatRecordOnFailureFunction));

    PCollectionTuple writeRecordsResult =
        convertMessagesResult
            .get(successfulConvertedRowsTag)
            .apply(
                "StorageApiWriteUnsharded",
                new StorageApiWriteUnshardedRecords<>(
                    dynamicDestinations,
                    bqServices,
                    failedRowsTag,
                    successfulWrittenRowsTag,
                    BigQueryStorageApiInsertErrorCoder.of(),
                    TableRowJsonCoder.of(),
                    autoUpdateSchema,
                    ignoreUnknownValues,
                    createDisposition,
                    kmsKey,
                    usesCdc,
                    defaultMissingValueInterpretation,
                    formatRecordOnFailureFunction));

    PCollection<BigQueryStorageApiInsertError> insertErrors =
        PCollectionList.of(convertMessagesResult.get(failedRowsTag))
            .and(writeRecordsResult.get(failedRowsTag))
            .apply("flattenErrors", Flatten.pCollections());

    @Nullable PCollection<TableRow> successfulWrittenRows = null;
    if (successfulWrittenRowsTag != null) {
      successfulWrittenRows = writeRecordsResult.get(successfulWrittenRowsTag);
    }

    addErrorCollections(convertMessagesResult, writeRecordsResult);

    return WriteResult.in(
        input.getPipeline(),
        null,
        null,
        null,
        null,
        null,
        failedRowsTag,
        insertErrors,
        successfulWrittenRowsTag,
        successfulWrittenRows);
  }

  private void addErrorCollections(
      PCollectionTuple convertMessagesResult, PCollectionTuple writeRecordsResult) {
    if (usesErrorHandler()) {
      PCollection<BadRecord> badRecords =
          PCollectionList.of(
                  convertMessagesResult
                      .get(failedRowsTag)
                      .apply(
                          "ConvertMessageFailuresToBadRecord",
                          ParDo.of(
                              new ConvertInsertErrorToBadRecord(
                                  "Failed to Convert to Storage API Message"))))
              .and(convertMessagesResult.get(BAD_RECORD_TAG))
              .and(
                  writeRecordsResult
                      .get(failedRowsTag)
                      .apply(
                          "WriteRecordFailuresToBadRecord",
                          ParDo.of(
                              new ConvertInsertErrorToBadRecord(
                                  "Failed to Write Message to Storage API"))))
              .apply("flattenBadRecords", Flatten.pCollections());
      badRecordErrorHandler.addErrorCollection(badRecords);
    }
  }

  private static class ConvertInsertErrorToBadRecord
      extends DoFn<BigQueryStorageApiInsertError, BadRecord> {

    private final String errorMessage;

    public ConvertInsertErrorToBadRecord(String errorMessage) {
      this.errorMessage = errorMessage;
    }

    @ProcessElement
    public void processElement(
        @Element BigQueryStorageApiInsertError bigQueryStorageApiInsertError,
        OutputReceiver<BadRecord> outputReceiver)
        throws IOException {
      outputReceiver.output(
          BadRecord.fromExceptionInformation(
              bigQueryStorageApiInsertError,
              BigQueryStorageApiInsertErrorCoder.of(),
              null,
              errorMessage));
    }
  }
}
