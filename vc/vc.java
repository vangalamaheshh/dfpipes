/*

@author: Mahesh Vangala
@email: vangalamaheshh@gmail.com
@date: Feb, 24, 2017
@copyright: Data Sciences & Technology, UMMS, 2017

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
import com.google.cloud.genomics.dockerflow.task.TaskDefn;
import com.google.cloud.genomics.dockerflow.task.TaskDefn.Param;

public class vc implements WorkflowDefn {
  static final String BWA_IMAGE = "docker.io/mvangala/bioifx_alignment_bwa:latest";
  
  static WorkflowArgs workflowArgs = ArgsBuilder.of()
    .input("BwaMem.sample_name", "${sample_name}")
    .build();
  
  @Override
  public Workflow createWorkflow(String[] args) throws IOException {
    return TaskBuilder.named(vc.class.getSimpleName())
      .steps(
        Steps.of(
          BwaMem
          )
        )
      .args(workflowArgs).build();
  }
  
  static Task BwaMem = TaskBuilder.named("BwaMem")
    .input("sample_name").scatterBy("sample_name")
    .inputFile("left_mate")
    .inputFile("right_mate")
    .inputFolder("bwa_ref_path", "gs://pipelines-api/ref-files/Homo-sapiens/b37/BWAIndex")
    .outputFile("bwa_out_sam", "${BwaMem.sample_name}.sam")
    .preemptible(true)
    .diskSize(50)
    .memory(16)
    .cpu(4)
    .docker(BWA_IMAGE)
    .script(
      "set -o pipefail\n" +
      "bwa mem -t 4 -R '@RG\\tID:${sample_name}\\tPU:${sample_name}\\tSM:{sample_name}\\tPL:ILLUMINA\\tLB:${sample_name}' \\\n" +
      "${bwa_ref_path}/b37 ${left_mate} ${right_mate} 1>${bwa_out_sam}"
    )
    .build();
}
