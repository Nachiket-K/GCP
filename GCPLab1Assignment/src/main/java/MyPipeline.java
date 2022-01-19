import com.google.gson.Gson;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineRunner;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.transforms.JsonToRow;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionTuple;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TupleTagList;
/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author user
 */
public class MyPipeline
{
    private static final Logger LOG = LoggerFactory.getLogger(MyPipeline.class);

    public static final TupleTag<CommonLog> VALID_M = new TupleTag<CommonLog>()
    {
        private static final long serialVrsnUID = 1L;
    };
    public static final TupleTag<String> INVALID_M = new TupleTag<String>()
    {
        private static final long serialVrsnUID = 1L;
    };

    /**public interface PipelineOptions extends DataflowPipelineOptions {
    @Description("BigQuery table name")
    String getTableName();
    void setTableName(String tableName);

    @Description("Input topic name")
    String getInputTopic();
    void setInputTopic(String inputTopic);

    @Description("DLQ topic name")
    String getdlqTopic();
    void setdlqTopic(String DlqTopic);


    @Description("input Subscription of PubSub")
    String getSubscription();
    void setSubscription(String subscription);
    }*/

    public static void main(String[] args) throws Exception
    {
        DataflowPipelineOptions PLoptions = PipelineOptionsFactory.fromArgs(args).withValidation().as(DataflowPipelineOptions.class);
        PLoptions.setJobName("usecase1-labid-5");
        PLoptions.setRegion("europe-west4");
        PLoptions.setGcpTempLocation("gs://c4e-uc1-dataflow-temp-5/temp");
        PLoptions.setStagingLocation("gs://c4e-uc1-dataflow-temp-5/staging");
        PLoptions.setWorkerMachineType("n1-standard-1");
        PLoptions.setSubnetwork("regions/europe-west4/subnetworks/subnet-uc1-5");
        PLoptions.setStreaming(true);
        PLoptions.setRunner(DataflowRunner.class);
        run(PLoptions);
    }
    public static final Schema RSchema = Schema
            .builder()
            .addInt32Field("id")
            .addStringField("name")
            .addStringField("surname")
            .build();
    public static class JsonToCommonLog extends DoFn<String, CommonLog>
    {
        private static final long serialVrsnUID = 1L;

        public static PCollectionTuple process(PCollection<String> input) throws Exception
        {
            return input.apply("JsonToCommonLog", ParDo.of(new DoFn<String, CommonLog>()
            {
                private static final long serialVrsnUID = 1L;

                @ProcessElement
                public void processElement(@Element String rec, ProcessContext Pcontext)
                {

                    try
                    {
                        Gson gson = new Gson();
                        CommonLog commonLg = gson.fromJson(rec, CommonLog.class);
                        Pcontext.output(VALID_M, commonLg);
                    }
                    catch (Exception e)
                    {
                        e.printStackTrace();
                        Pcontext.output(INVALID_M, rec);
                    }
                }
            }).withOutputTags(VALID_M, TupleTagList.of(INVALID_M)));
        }
    }
    public static void run(DataflowPipelineOptions Poptions) throws Exception
    {
        //String PSsubscriptionName="projects/"+Poptions.getProject()+"/subscriptions/"+Poptions.getSubscription();
        //String oTableName=Poptions.getProject()+":"+Poptions.getTableName();
        // Create the pipeline
        Pipeline Npipeline = Pipeline.create(Poptions);

        PCollection<String> DataOne=Npipeline.apply("ReadMessageFromPubSub", PubsubIO.readStrings().fromSubscription("projects/nttdata-c4e-bde/subscriptions/uc1-input-topic-sub-5"));
        PCollectionTuple PCtuple= JsonToCommonLog.process(DataOne);

        PCtuple.get(VALID_M).apply("CommonLogToJson", ParDo.of(new DoFn<CommonLog, String>()
        {
            private static final long serialVrsnUID = 1L;

            @ProcessElement
            public void processElement(ProcessContext Pcontext)
            {
                Gson gsonObj = new Gson();
                String jsnRec = gsonObj.toJson(Pcontext.element());
                Pcontext.output(jsnRec);
            }
        })).apply("JsontoRow", JsonToRow.withSchema(RSchema)).apply("InserttoBigQuery",BigQueryIO.<Row>write().to("nttdata-c4e-bde:uc1_5.account")
                .useBeamSchema().withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_NEVER).withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));

        PCtuple.get(INVALID_M).apply("WriteToDLQTopic", PubsubIO.writeStrings().to("projects/nttdata-c4e-bde/topics/uc1-dlq-topic-5"));
        Npipeline.run().waitUntilFinish();


    }
}