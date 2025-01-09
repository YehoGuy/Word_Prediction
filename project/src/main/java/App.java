import Helpers.Consts;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.HadoopJarStepConfig;
import com.amazonaws.services.elasticmapreduce.model.JobFlowInstancesConfig;
import com.amazonaws.services.elasticmapreduce.model.PlacementType;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowRequest;
import com.amazonaws.services.elasticmapreduce.model.RunJobFlowResult;
import com.amazonaws.services.elasticmapreduce.model.StepConfig;

public class App {
    public static AWSCredentialsProvider credentialsProvider;
    public static AmazonElasticMapReduce emr;
    public static int numberOfInstances = 5;

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
                .withJar(Consts.STEP1_JAR)
                .withMainClass("Step1");

        StepConfig stepConfig1 = new StepConfig()
                .withName("Step1")
                .withHadoopJarStep(step1)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        // Step 2
        HadoopJarStepConfig step2 = new HadoopJarStepConfig()
                .withJar(Consts.STEP2_JAR)
                .withMainClass("Step2");

        StepConfig stepConfig2 = new StepConfig()
                .withName("Step2")
                .withHadoopJarStep(step2)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        // Step 3
        HadoopJarStepConfig step3 = new HadoopJarStepConfig()
                .withJar(Consts.STEP3_JAR)
                .withMainClass("Step3");

        StepConfig stepConfig3 = new StepConfig()
                .withName("Step3")
                .withHadoopJarStep(step3)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        // Step 4
        HadoopJarStepConfig step4 = new HadoopJarStepConfig()
                .withJar(Consts.STEP4_JAR)
                .withMainClass("Step4");

        StepConfig stepConfig4 = new StepConfig()
                .withName("Step4")
                .withHadoopJarStep(step4)
                .withActionOnFailure("TERMINATE_JOB_FLOW");

        // Step 5
        HadoopJarStepConfig step5 = new HadoopJarStepConfig()
                .withJar(Consts.STEP5_JAR)
                .withMainClass("Step5");

        StepConfig stepConfig5 = new StepConfig()
                .withName("Step5")
                .withHadoopJarStep(step5)
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
                .withSteps(stepConfig1,stepConfig2,stepConfig3,stepConfig4,stepConfig5)
                .withLogUri(Consts.logs)
                .withServiceRole("EMR_DefaultRole")
                .withJobFlowRole("EMR_EC2_DefaultRole")
                .withReleaseLabel("emr-5.11.0");

        RunJobFlowResult runJobFlowResult = emr.runJobFlow(runFlowRequest);
        String jobFlowId = runJobFlowResult.getJobFlowId();
        System.out.println("[final] Ran job flow with id: " + jobFlowId);
    }
}
