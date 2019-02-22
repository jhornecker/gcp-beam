package com.aticsi.sample;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;

import com.google.cloud.bigtable.beam.CloudBigtableIO;
import com.google.cloud.bigtable.beam.CloudBigtableTableConfiguration;
import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import com.google.gson.GsonBuilder;

/**
 * Dataflow template which copies Pubsub Messages to Datastore. This expects
 * Pubsub messages to contain JSON text in the v1/Entity rest format:
 * https://cloud.google.com/datastore/docs/reference/rest/v1/Entity
 */
public class StarterPipeline {

	static String PROJECT_ID = "aticsi-pubsub";
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
//		DataflowPipelineOptions options = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
		PipelineOptions options = PipelineOptionsFactory.fromArgs(args).as(PipelineOptions.class);
//		options.setProject(PROJECT_ID);
//		options.setRunner(DataflowRunner.class);
//		options.setStreaming(true);
//		options.setGcpCredential(credentials);
//		options.setGcpTempLocation("gs://aticsi-bucket/tmp/");

		Pipeline pipeline = Pipeline.create(options);

//		pipeline.apply(Create.of("Hello", "World"));
//
		CloudBigtableTableConfiguration config = new CloudBigtableTableConfiguration.Builder().withProjectId(PROJECT_ID)
				.withInstanceId(BIGTABLE_INSTANCE_ID).withTableId(TABLE_ID).build();
//
		pipeline.apply(PubsubIO.readStrings().fromSubscription(SUBS)).apply(ParDo.of(MUTATION_TRANSFORM))
				.apply(CloudBigtableIO.writeToTable(config));

		pipeline.run();
	}

	// [START bigtable_dataflow_connector_process_element]
	static final DoFn<String, Mutation> MUTATION_TRANSFORM = new DoFn<String, Mutation>() {
		private static final long serialVersionUID = 1L;

		@ProcessElement
		public void processElement(DoFn<String, Mutation>.ProcessContext c) throws Exception {
			System.out.println("process element " + c.element());
//			String message = c.element().getData();
			CartEvent event = new GsonBuilder().create().fromJson(c.element(), CartEvent.class);
			Put put = new Put((event.getEan() + "#" + System.currentTimeMillis()).getBytes());
			put.addColumn("CF".getBytes(), "EAN".getBytes(), event.getEan().getBytes());
			put.addColumn("CF".getBytes(), "QTE".getBytes(), event.getQte().getBytes());
			put.addColumn("CF".getBytes(), "EVENT_ID".getBytes(), event.getEventID().getBytes());

//			 try (Connection connection = BigtableConfiguration.connect(PROJECT_ID, BIGTABLE_INSTANCE_ID)) {
//
//			      // Create a connection to the table that already exists
//			      // Use try-with-resources to make sure the connection to the table is closed correctly
//			      try (Table table = connection.getTable(TableName.valueOf(TABLE_ID))) {
//
//			    	  
//			    	  table.put(put);
//			    	  
//
//			        System.out.printf("ligne inseree");
//
//			      }  catch (IOException e) {
//			    	  e.printStackTrace();
//			        // handle exception while connecting to a table
//			        throw e;
//			      }
//			    } catch (IOException e) {
//			    	e.printStackTrace();
//			      System.err.println("Exception while running quickstart: " + e.getMessage());
//			      e.printStackTrace();
//			    }

			c.output(put);
		}
	};

}