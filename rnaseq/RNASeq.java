/*

@author: Mahesh Vangala
@email: vangalamaheshh@gmail.com
@date: Feb, 24, 2017
@copyright: Mahesh Vangala 2017

**/

import java.io.IOException;
import java.util.Map;

import com.google.cloud.genomics.dockerflow.args.ArgsBuilder;
import com.google.cloud.genomics.dockerflow.args.WorkflowArgs;
import com.google.cloud.genomics.dockerflow.task.Task;
import com.google.cloud.genomics.dockerflow.task.TaskBuilder;
import com.google.cloud.genomics.dockerflow.workflow.Workflow;
import com.google.cloud.genomics.dockerflow.workflow.Workflow.Branch;
import com.google.cloud.genomics.dockerflow.workflow.Workflow.Steps;
import com.google.cloud.genomics.dockerflow.workflow.WorkflowDefn;

public class RNASeq implements WorkflowDefn {
  static final String TRIM_IMAGE = "docker.io/mvangala/bioifx_preprocess_trimmomatic:0.0.1";

  static Task Trimmomatic = TaskBuilder.named("Trimmomatic")
      .input("sample_name").scatterBy("sample_name")
      .inputFile("leftmate", "gs://testdf/input/rnaseq/${sample_name}_R1.fastq.gz")
      .inputFile("rightmate", "gs://testdf/input/rnaseq/${sample_name}_R2.fastq.gz")
      .outputFile("leftmateP", "${sample_name}.left.paired.trim.fastq.gz")
      .outputFile("leftmateU", "${sample_name}.left.unpaired.trim.fastq.gz")
      .outputFile("rightmateP", "${sample_name}.right.paired.trim.fastq.gz")
      .outputFile("rightmateU", "${sample_name}.right.unpaired.trim.fastq.gz")
      .outputFile("logfile", "${sample_name}.trim.log")
      .preemptible(true)
      .diskSize("${agg_small_disk}")
      .memory(4)
      .cpu(16)
      .docker(TRIM_IMAGE)
      .script(
        "set -o pipefail\n" +
	"TrimmomaticPE -threads 16 $leftmate $rightmate $leftmateP $leftmateU $rightmateP $rightmateU ILLUMINACLIP:/usr/share/trimmomatic/TruSeq2-PE.fa:2:30:10 LEADING:3 TRAILING:3 SLIDINGWINDOW:4:20 MINLEN:36 >&${logfile}"
       )
      .build();

  static Task TrimGather = TaskBuilder.named("TrimGather")
      .script("#do nothing")
      .input("pipelinerun", "${workflow.index}").gatherBy("pipelinerun")
      .build();

  static Task STAR = TaskBuilder.named("STAR")
      .script("#do nothing")
      .build();

  static Task STARGather = TaskBuilder.named("STARGather")
      .script("#do nothing")
      .input("pipelinerun", "${workflow.index}").gatherBy("pipelinerun")
      .build();
	
  static WorkflowArgs workflowArgs = ArgsBuilder.of()
      .input("Trimmomatic.sample_name", "${sample_name}")
      .build();

  @Override
  public Workflow createWorkflow(String[] args) throws IOException {
    return TaskBuilder.named(RNASeq.class.getSimpleName())
      .steps(
        Steps.of(
          Trimmomatic,
          Branch.of(
            TrimGather,
            Steps.of(
              STAR,
              STARGather
            )
          )
        )
      )
      .args(workflowArgs).build();
  }
}
