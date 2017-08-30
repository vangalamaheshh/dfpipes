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
  static final String SAM_IMAGE = "docker.io/mvangala/bioifx_format_samtools:latest";
  static final String PICARD_IMAGE = "docker.io/mvangala/bioifx_preprocess_picard:latest";
  static final String JAVA8_IMAGE = "docker.io/mvangala/base-java8:latest";
  
  static WorkflowArgs workflowArgs = ArgsBuilder.of()
    .input("BwaMem.sample_name", "${sample_name}")
    .build();
  
  @Override
  public Workflow createWorkflow(String[] args) throws IOException {
    return TaskBuilder.named(vc.class.getSimpleName())
      .steps(
        Steps.of(
          BwaMem,
          Sam2SortedBam,
          MarkDups
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
    .diskSize(100)
    .memory(14)
    .cpu(4)
    .docker(BWA_IMAGE)
    .script(
      "set -o pipefail\n" +
      "bwa mem -t 4 -R '@RG\\tID:${sample_name}\\tPU:${sample_name}\\tSM:{sample_name}\\tPL:ILLUMINA\\tLB:${sample_name}' \\\n" +
      "${bwa_ref_path}/b37 ${left_mate} ${right_mate} 1>${bwa_out_sam}"
    )
    .build();

  static Task Sam2SortedBam = TaskBuilder.named("Sam2SortedBam")
    .input("sample_name", "${BwaMem.sample_name}")
    .inputFile("in_sam", "${BwaMem.bwa_out_sam}")
    .outputFile("out_sorted_bam", "${BwaMem.sample_name}.sorted.bam")
    .outputFile("out_sorted_bam_index", "${BwaMem.sample_name}.sorted.bam.bai")
    .preemptible(true)
    .diskSize(100)
    .memory(14)   
    .cpu(4)
    .docker(SAM_IMAGE)
    .script(
      "set -o pipefail \n" +
      "samtools view -bS -@ 4 ${in_sam} 1>${sample_name}.bam \n" +
      "samtools sort -@ 4 -m 3G -f ${sample_name}.bam ${out_sorted_bam} \n" +
      "samtools index ${out_sorted_bam}"
    )
    .build();
  
  static Task MarkDups = TaskBuilder.named("MarkDups")
    .input("sample_name", "${BwaMem.sample_name}")
    .inputFile("in_sorted_bam", "${Sam2SortedBam.out_sorted_bam}")
    .outputFile("dedup_bam", "${BwaMem.sample_name}.dedup.bam")
    .outputFile("dedup_bam_index", "${BwaMem.sample_name}.dedup.bai")
    .outputFile("metrics_file", "${BwaMem.sample_name}.metrics.txt")
    .preemptible(true)
    .diskSize(100)
    .memory(12)
    .cpu(2)
    .docker(PICARD_IMAGE)
    .script(
      "set -o pipefail \n" +
      "picard-tools MarkDuplicates I=${in_sorted_bam} O=${dedup_bam} METRICS_FILE=${metrics_file} \n" +
      "picard-tools BuildBamIndex INPUT=${dedup_bam} "
    )
    .build();

  static Task   
}
