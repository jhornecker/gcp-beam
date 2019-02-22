package com.aticsi.sample;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;

/*


 * 
 * 
 * Copyright (C) 2018 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); youmay not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;

import com.google.api.client.util.Lists;
import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.bigtable.beam.CloudBigtableIO;
import com.google.cloud.bigtable.beam.CloudBigtableTableConfiguration;
import com.google.gson.GsonBuilder;

/**
 * Dataflow template which copies Pubsub Messages to Datastore. This expects
 * Pubsub messages to contain JSON text in the v1/Entity rest format:
 * https://cloud.google.com/datastore/docs/reference/rest/v1/Entity
 */
public class StarterPipeline {

	static String PROJECT_ID = "acticsi-pubsub";
	static String BIGTABLE_INSTANCE_ID = "bigtable-inst-1";
	static String TABLE_ID = "CartEvent";
	private static final String SUBS = "projects/aticsi-pubsub/subscriptions/my-sub";

	/**
	 * Runs a pipeline which reads in JSON from Pubsub, feeds the JSON to a
	 * Javascript UDF, and writes the JSON encoded Entities to Datastore.
	 *
	 * @param args arguments to the pipeline
	 * @throws IOException 
	 * @throws FileNotFoundException 
	 */
	public static void main(String[] args) throws FileNotFoundException, IOException {
		DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);

		options.setProject(PROJECT_ID);
		options.setRunner(DataflowRunner.class);
		options.setStreaming(true);
		GoogleCredentials credentials = GoogleCredentials.fromStream(new FileInputStream("src/main/resources/aticsi-pubsub.json"))
				.createScoped(
						com.google.common.collect.Lists.newArrayList("https://www.googleapis.com/auth/cloud-platform"));
		options.setGcpCredential(credentials);
		options.setGcpTempLocation("gs://dataflow-bk-1/tmp/");

		Pipeline pipeline = Pipeline.create(options);

		CloudBigtableTableConfiguration config = new CloudBigtableTableConfiguration.Builder().withProjectId(PROJECT_ID)
				.withInstanceId(BIGTABLE_INSTANCE_ID).withTableId(TABLE_ID).build();

		pipeline.apply(PubsubIO.readStrings().fromSubscription(SUBS)).apply(ParDo.of(MUTATION_TRANSFORM))
				.apply(CloudBigtableIO.writeToTable(config));

		pipeline.run();
	}

	// [START bigtable_dataflow_connector_process_element]
	static final DoFn<String, Mutation> MUTATION_TRANSFORM = new DoFn<String, Mutation>() {
		private static final long serialVersionUID = 1L;

		@ProcessElement
		public void processElement(DoFn<String, Mutation>.ProcessContext c) throws Exception {
//			String message = c.element().getData();
			CartEvent event = new GsonBuilder().create().fromJson(c.element(), CartEvent.class);
			Put put = new Put((event.getEan() + "#" + System.currentTimeMillis()).getBytes());
			put.addColumn("CF".getBytes(), "EAN".getBytes(), event.getEan().getBytes());
			put.addColumn("CF".getBytes(), "QTE".getBytes(), event.getQte().getBytes());
			put.addColumn("CF".getBytes(), "EVENT_ID".getBytes(), event.getEventID().getBytes());
			c.output(put);
		}
	};

}