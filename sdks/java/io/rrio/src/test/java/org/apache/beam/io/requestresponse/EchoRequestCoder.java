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
package org.apache.beam.io.requestresponse;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.testinfra.mockapis.echo.v1.Echo.EchoRequest;
import org.checkerframework.checker.nullness.qual.NonNull;

class EchoRequestCoder extends CustomCoder<@NonNull EchoRequest> {

  @Override
  public void encode(@NonNull EchoRequest value, @NonNull OutputStream outStream)
      throws CoderException, IOException {
    value.writeTo(outStream);
  }

  @Override
  public @NonNull EchoRequest decode(@NonNull InputStream inStream)
      throws CoderException, IOException {
    return EchoRequest.parseFrom(inStream);
  }

  @Override
  public void verifyDeterministic() throws NonDeterministicException {}
}
