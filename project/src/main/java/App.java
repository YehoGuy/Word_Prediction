import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig;
import com.amazonaws.services.elasticmapreduce.model.PlacementType;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowResult;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

public class App {
    public static AWSCredentialsProvider credentialsProvider;
    public static AmazonElasticMapReduce emr;
    public static int numberOfInstances = 8;

    public static void main(String[]args){
        //int lineLimit = -1;
        int lineLimit = 100;
        if(args.length>0){
            try{
                lineLimit=Integer.parseInt(args[0]);
                System.out.println("[info] Entry-Limit found and set to "+lineLimit);
            }catch(Exception e){
                System.err.println("[info] Unable to parse Entry-Limit: "+args[0]);
            }
        }
        credentialsProvider = new ProfileCredentialsProvider();
        System.out.println("[INFO] Starting System.");
        emr = AmazonElasticMapReduceClientBuilder.standard()
                .withCredentials(credentialsProvider)
                .withRegion("us-east-1")
                .build();
        System.out.println("[INFO] Connecting to aws succeeded.");
        System.out.print( "existing clusters: ");
        System.out.println(emr.listClusters());

        // Step 1
        HadoopJarStepConfig step1 = new HadoopJarStepConfig()
                .withJar("s3://guyss3bucketfordistributedsystems/jars/Step1test.jar")
                .withMainClass("Step1")
                .withArgs(""+lineLimit);

        StepConfig stepConfig1 = new StepConfig()
                .withName("Step1")
                .withHadoopJarStep(step1)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        //Job flow
        JobFlowInstancesConfig instances = new JobFlowInstancesConfig()
                .withInstanceCount(numberOfInstances)
                .withMasterInstanceType(InstanceType.M4Large.toString())
                .withSlaveInstanceType(InstanceType.M4Large.toString())
                .withHadoopVersion("2.9.2")
                .withEc2KeyName("vockey")
                .withKeepJobFlowAliveWhenNoSteps(false)
                .withPlacement(new PlacementType("us-east-1a"));

        System.out.println("[info] Steps set successfully.");
        RunJobFlowRequest runFlowRequest = new RunJobFlowRequest()
                .withName("Word Prediction project")
                .withInstances(instances)
                .withSteps(stepConfig1)
                .withLogUri("s3://guyss3bucketfordistributedsystems/logs/")
                .withServiceRole("EMR_DefaultRole")
                .withJobFlowRole("EMR_EC2_DefaultRole")
                .withReleaseLabel("emr-5.11.0");

        RunJobFlowResult runJobFlowResult = emr.runJobFlow(runFlowRequest);
        String jobFlowId = runJobFlowResult.getJobFlowId();
        System.out.println("[final] Ran job flow with id: " + jobFlowId);
    }
}
