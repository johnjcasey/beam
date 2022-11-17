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

package org.apache.beam.sdk.io.camel;

import com.google.auto.value.AutoValue;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;
import org.apache.camel.impl.DefaultCamelContext;
import org.apache.camel.impl.engine.DefaultProducerTemplate;


public class CamelIO {

  public static <K> Write<K> write() {
    return new AutoValue_CamelIO_Write.Builder<K>().build();
  }

  @AutoValue
  @AutoValue.CopyAnnotations
  public abstract static class Write<K> extends PTransform<PCollection<K>, PCollection<K>> {

    abstract Builder<K> toBuilder();

    @Experimental(Experimental.Kind.PORTABILITY)
    @AutoValue.Builder
    abstract static class Builder<K> {

      abstract Write<K> build();
    }

    @Override
    public PCollection<K> expand(PCollection<K> input) {
      return input.apply(ParDo.of(new WriteFn<K>()));
    }

  }

  private static class WriteFn<K> extends DoFn<K,K> {
    ProducerTemplate template;

    private String endpoint;
    private Map<String,String> options;

    public WriteFn(String endpoint, Map<String, String> options){
      this.endpoint = endpoint;
      this.options = options;
    }

    @StartBundle
    private void startBundle(){
      CamelContext context = new DefaultCamelContext();
      template = new DefaultProducerTemplate(context, createEndpoint(endpoint,options));
    }

    @ProcessElement
    private void processElement(@Element K element, OutputReceiver<K> receiver){
      template.sendBody(element);
      receiver.output(element);
    }

    @FinishBundle
    private void finishBundle(){
      template.stop();
    }

    private String createEndpoint(String endpoint, Map<String,String> options){
      StringBuilder builder = new StringBuilder();
      builder.append(endpoint);
      builder.append("?");
      for (Entry<String,String> entry : options.entrySet()){
        builder.append(entry.getKey());
        builder.append("=");
        builder.append(entry.getValue());
        builder.append("&");
      }
      return builder.toString();
    }
  }

}
