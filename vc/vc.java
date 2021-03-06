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
  static final String GCLOUD_IMAGE = "docker.io/mvangala/bioifx_postprocess_vc:latest";
  
  static WorkflowArgs workflowArgs = ArgsBuilder.of()
    .input("BwaMem.sample_name", "${sample_name}")
    .input("input_url", "${input_url}")
    .input("workspace", "${workspace}")
    .input("project_id", "${project_id}")
    .input("paired_end", "${paired_end}")
    .input("bq_dataset_name_gx", "${bq_dataset_name_gx")
    .build();
  
  @Override
  public Workflow createWorkflow(String[] args) throws IOException {
    return TaskBuilder.named(vc.class.getSimpleName())
      .steps(
        Steps.of(
          BwaMem,
          Sam2SortedBam,
          MarkDups,
          BQSR,
          HaplotypeCaller,
          LoadVariants2BQ
        )
      )
      .args(workflowArgs).build();
  }
  
  static Task BwaMem = TaskBuilder.named("BwaMem")
    .input("sample_name").scatterBy("sample_name")
    .input("paired_end", "${paired_end}")
    .inputFolder("fastq_folder", "${input_url}/${BwaMem.sample_name}")
    .inputFolder("bwa_ref_path", "gs://pipelines-api/ref-files/Homo-sapiens/b37/BWAIndex")
    .outputFile("bwa_out_sam", "${BwaMem.sample_name}.sam")
    .preemptible(true)
    .diskSize(200)
    .memory(14)
    .cpu(4)
    .docker(BWA_IMAGE)
    .script(
      "set -o pipefail\n" +
      "sample_name=${sample_name}\n" +
      "if [ ${paired_end} == \"true\" ]; then \n" +
        "fastq_files=(${fastq_folder}/*_R1*fastq.gz ${fastq_folder}/*_R2*fastq.gz)\n" +
      "else\n" +
        "fastq_files=(${fastq_folder}/*_R1*fastq.gz)\n" +
      "fi\n" +
      "bwa mem -t 4 -R \"@RG\\tID:${sample_name}\\tPU:${sample_name}\\tSM:${sample_name}\\tPL:ILLUMINA\\tLB:${sample_name}\" \\\n" +
      "${bwa_ref_path}/b37 ${fastq_files[*]} 1>${bwa_out_sam}"
    )
    .build();

  static Task Sam2SortedBam = TaskBuilder.named("Sam2SortedBam")
    .input("sample_name", "${BwaMem.sample_name}")
    .inputFile("in_sam", "${BwaMem.bwa_out_sam}")
    .outputFile("out_sorted_bam", "${BwaMem.sample_name}.sorted.bam")
    .outputFile("out_sorted_bam_index", "${BwaMem.sample_name}.sorted.bam.bai")
    .preemptible(true)
    .diskSize(200)
    .memory(14)   
    .cpu(4)
    .docker(SAM_IMAGE)
    .script(
      "set -o pipefail \n" +
      "samtools view -bS -@ 4 ${in_sam} 1>${sample_name}.bam \n" +
      "samtools sort -o ${out_sorted_bam} -T ${sample_name} ${sample_name}.bam \n" +
      "samtools index ${out_sorted_bam} "
    )
    .build();
  
  static Task MarkDups = TaskBuilder.named("MarkDups")
    .input("sample_name", "${BwaMem.sample_name}")
    .inputFile("in_sorted_bam", "${Sam2SortedBam.out_sorted_bam}")
    .outputFile("dedup_bam", "${BwaMem.sample_name}.dedup.bam")
    .outputFile("dedup_bam_index", "${BwaMem.sample_name}.dedup.bai")
    .outputFile("metrics_file", "${BwaMem.sample_name}.metrics.txt")
    .preemptible(true)
    .diskSize(200)
    .memory(12)
    .cpu(2)
    .docker(PICARD_IMAGE)
    .script(
      "set -o pipefail \n" +
      "picard-tools MarkDuplicates I=${in_sorted_bam} O=${dedup_bam} METRICS_FILE=${metrics_file} \\\n" +
      "TMP_DIR=/mnt/data \n" +
      "picard-tools BuildBamIndex INPUT=${dedup_bam} "
    )
    .build();

  static Task BQSR = TaskBuilder.named("BQSR")
    .input("sample_name", "${BwaMem.sample_name}")
    .inputFile("gatk_jar", "gs://pipelines-api/software/gatk/GenomeAnalysisTK.jar")
    .inputFile("ref_fa", "gs://pipelines-api/ref-files/Homo-sapiens/b37/fasta/Homo_sapiens_assembly19.fasta")
    .inputFile("ref_fa_idx", "gs://pipelines-api/ref-files/Homo-sapiens/b37/fasta/Homo_sapiens_assembly19.fasta.fai")
    .inputFile("ref_fa_dict", "gs://pipelines-api/ref-files/Homo-sapiens/b37/fasta/Homo_sapiens_assembly19.dict")
    .inputFile("dbSNP", "gs://pipelines-api/ref-files/Homo-sapiens/b37/dbSNP/dbsnp_138.b37.vcf")
    .inputFile("dbSNP_idx", "gs://pipelines-api/ref-files/Homo-sapiens/b37/dbSNP/dbsnp_138.b37.vcf.idx")
    .inputFile("dedup_bam", "${MarkDups.dedup_bam}")
    .inputFile("dedup_bam_idx", "${MarkDups.dedup_bam_index}")
    .outputFile("recal_data", "${BwaMem.sample_name}.recal_data.table")
    .outputFile("post_recal_data", "${BwaMem.sample_name}.post_recal_data.table")
    .outputFile("bqsr_bam", "${BwaMem.sample_name}.recal_reads.bam")
    .outputFile("bqsr_bam_idx", "${BwaMem.sample_name}.recal_reads.bai")
    .preemptible(true)
    .diskSize(200)
    .memory(14)
    .cpu(4)
    .docker(PICARD_IMAGE)
    .script(
      "set -o pipefail \n" +
      "java -jar ${gatk_jar} -T BaseRecalibrator -R ${ref_fa} -I ${dedup_bam} \\\n" +
      "-knownSites ${dbSNP} -o ${recal_data} -nct 4 \n" +
      "java -jar ${gatk_jar} -T BaseRecalibrator -R ${ref_fa} -I ${dedup_bam} \\\n" +
      "-knownSites ${dbSNP} -BQSR ${recal_data} -o ${post_recal_data} -nct 4 \n" +
      "java -jar ${gatk_jar} -T PrintReads -R ${ref_fa} -I ${dedup_bam} \\\n" +
      "-BQSR ${recal_data} -o ${bqsr_bam} -nct 4 \n" +
      "picard-tools BuildBamIndex INPUT=${bqsr_bam} "   
    )
    .build();

  static Task HaplotypeCaller = TaskBuilder.named("HaplotypeCaller")
    .input("sample_name", "${BwaMem.sample_name}")
    .inputFile("gatk_jar", "gs://pipelines-api/software/gatk/GenomeAnalysisTK.jar")
    .inputFile("ref_fa", "gs://pipelines-api/ref-files/Homo-sapiens/b37/fasta/Homo_sapiens_assembly19.fasta")
    .inputFile("ref_fa_idx", "gs://pipelines-api/ref-files/Homo-sapiens/b37/fasta/Homo_sapiens_assembly19.fasta.fai")
    .inputFile("ref_fa_dict", "gs://pipelines-api/ref-files/Homo-sapiens/b37/fasta/Homo_sapiens_assembly19.dict")
    .inputFile("dbSNP", "gs://pipelines-api/ref-files/Homo-sapiens/b37/dbSNP/dbsnp_138.b37.vcf")
    .inputFile("dbSNP_idx", "gs://pipelines-api/ref-files/Homo-sapiens/b37/dbSNP/dbsnp_138.b37.vcf.idx")
    .inputFile("bqsr_bam", "${BQSR.bqsr_bam}")
    .inputFile("bqsr_bam_idx", "${BQSR.bqsr_bam_idx}")
    .outputFile("out_vcf", "${BwaMem.sample_name}.vcf")
    .outputFile("out_vcf_idx", "${BwaMem.sample_name}.vcf.idx")
    .preemptible(true)
    .diskSize(200)
    .memory(14)
    .cpu(4)
    .docker(PICARD_IMAGE)
    .script(
      "set -o pipefail \n" +
      "java -jar ${gatk_jar} -T HaplotypeCaller -R ${ref_fa} -I ${bqsr_bam} \\\n" +
      "--dbsnp ${dbSNP} -o /mnt/data/${sample_name}.haplotype.vcf -nct 4 \n" +
      "java -jar ${gatk_jar} -T VariantFiltration --variant /mnt/data/${sample_name}.haplotype.vcf \\\n" +
      "-o ${out_vcf} --filterExpression 'QD < 2.0 || FS > 60.0 || MQ < 40.0 || MQRankSum < -12.5 || ReadPosRankSum < -8.0' \\\n" + 
      "--filterName 'synergist-default-snp-filter' -R ${ref_fa} "
    )
    .input("pipeline_run", "${workflow.index}")
    .gatherBy("pipeline_run")
    .build();

  static Task LoadVariants2BQ = TaskBuilder.named("LoadVariants2BQ")
    .input("project_id", "${project_id}")
    .input("bq_dataset_name_gx", "${bq_dataset_name_gx}")
    .input("workspace", "${workspace}/HaplotypeCaller")
    .inputFile("gmx_file", "gs://pipelines-api/keys/gmx.json")
    .outputFile("out_file", "${project_id}.load_variants.done")
    .preemptible(true)
    .diskSize(1)
    .memory("0.5")
    .cpu(1)
    .docker(GCLOUD_IMAGE)
    .script(
      "set -o pipefail \n" +
      "project_id=${project_id} workspace=${workspace} \\\n" +
      "gmx_file=${gmx_file} out_file=${out_file} bq_dataset_name_gx=${bq_dataset_name_gx} \\\n" +
      "bash /load_variants.bash "     
    ).build();
}
