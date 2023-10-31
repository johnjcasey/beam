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
package org.apache.beam.sdk.errorhandling;

import static org.apache.beam.sdk.errorhandling.BadRecordRouter.BAD_RECORD_TAG;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import org.apache.beam.sdk.coders.BigEndianIntegerCoder;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.errorhandling.BadRecord.Failure;
import org.apache.beam.sdk.errorhandling.BadRecord.Record;
import org.apache.beam.sdk.transforms.DoFn.MultiOutputReceiver;
import org.apache.beam.sdk.transforms.DoFn.OutputReceiver;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

@RunWith(JUnit4.class)
public class BadRecordRouterTest {

  @Rule public final MockitoRule mockito = MockitoJUnit.rule();

  @Rule public ExpectedException thrown = ExpectedException.none();

  @Mock private MultiOutputReceiver outputReceiver;

  @Mock private OutputReceiver<BadRecord> badRecordOutputReceiver;

  @Test
  public void testThrowingHandlerWithException() throws Exception {
    BadRecordRouter handler = BadRecordRouter.THROWING_ROUTER;

    thrown.expect(RuntimeException.class);

    handler.route(outputReceiver, new Object(), null, new RuntimeException(), "desc", "transform");
  }

  @Test
  public void testThrowingHandlerWithNoException() throws Exception {
    BadRecordRouter handler = BadRecordRouter.THROWING_ROUTER;

    handler.route(outputReceiver, new Object(), null, null, "desc", "transform");
  }

  @Test
  public void testRecordingHandler() throws Exception {
    when(outputReceiver.get(BAD_RECORD_TAG)).thenReturn(badRecordOutputReceiver);

    BadRecordRouter handler = BadRecordRouter.RECORDING_ROUTER;

    handler.route(
        outputReceiver, 5, BigEndianIntegerCoder.of(), new RuntimeException(), "desc", "transform");

    BadRecord expected =
        BadRecord.builder()
            .setRecord(
                Record.builder()
                    .setHumanReadableRecord("5")
                    .setEncodedRecord(new byte[] {0, 0, 0, 5})
                    .setCoder("BigEndianIntegerCoder")
                    .build())
            .setFailure(
                Failure.builder()
                    .setException("java.lang.RuntimeException")
                    .setDescription("desc")
                    .setFailingTransform("transform")
                    .build())
            .build();

    verify(badRecordOutputReceiver).output(expected);
  }

  @Test
  public void testNoCoder() throws Exception {
    when(outputReceiver.get(BAD_RECORD_TAG)).thenReturn(badRecordOutputReceiver);

    BadRecordRouter handler = BadRecordRouter.RECORDING_ROUTER;

    handler.route(outputReceiver, 5, null, new RuntimeException(), "desc", "transform");

    BadRecord expected =
        BadRecord.builder()
            .setRecord(Record.builder().setHumanReadableRecord("5").build())
            .setFailure(
                Failure.builder()
                    .setException("java.lang.RuntimeException")
                    .setDescription("desc")
                    .setFailingTransform("transform")
                    .build())
            .build();

    verify(badRecordOutputReceiver).output(expected);
  }

  @Test
  public void testFailingCoder() throws Exception {
    when(outputReceiver.get(BAD_RECORD_TAG)).thenReturn(badRecordOutputReceiver);

    BadRecordRouter handler = BadRecordRouter.RECORDING_ROUTER;

    Coder<Integer> failingCoder =
        new Coder<Integer>() {
          @Override
          public void encode(Integer value, OutputStream outStream)
              throws CoderException, IOException {
            throw new IOException();
          }

          @Override
          public Integer decode(InputStream inStream) throws CoderException, IOException {
            return null;
          }

          @Override
          public List<? extends Coder<?>> getCoderArguments() {
            return null;
          }

          @Override
          public void verifyDeterministic() throws NonDeterministicException {}
        };

    handler.route(outputReceiver, 5, failingCoder, new RuntimeException(), "desc", "transform");

    BadRecord expected =
        BadRecord.builder()
            .setRecord(
                Record.builder()
                    .setHumanReadableRecord("5")
                    .setCoder(failingCoder.toString())
                    .build())
            .setFailure(
                Failure.builder()
                    .setException("java.lang.RuntimeException")
                    .setDescription("desc")
                    .setFailingTransform("transform")
                    .build())
            .build();

    verify(badRecordOutputReceiver).output(expected);
  }
}
