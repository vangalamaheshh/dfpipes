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
          MarkDups,
          BQSR,
          HaplotypeCaller
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
    .diskSize(200)
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
    .diskSize(200)
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

  static Task BQSR = TaskBuilder.named("BQSR")
    .input("sample_name", "${BwaMem.sample_name}")
    .inputFile("gatk_jar", "gs://pipelines-api/software/gatk/GenomeAnalysisTK.jar")
    .inputFile("ref_fa", "gs://pipelines-api/ref-files/Homo-sapiens/b37/fasta/Homo_sapiens_assembly19.fasta")
    .inputFile("ref_fa_idx", "gs://pipelines-api/ref-files/Homo-sapiens/b37/fasta/Homo_sapiens_assembly19.fasta.fai")
    .inputFile("ref_fa_dict", "gs://pipelines-api/ref-files/Homo-sapiens/b37/fasta/Homo_sapiens_assembly19.dict")
    .inputFile("dbSNP", "gs://pipelines-api/ref-files/Homo-sapiens/b37/dbSNP/dbsnp_138.b37.vcf.gz")
    .inputFile("dbSNP_idx", "gs://pipelines-api/ref-files/Homo-sapiens/b37/dbSNP/dbsnp_138.b37.vcf.idx.gz")
    .inputFile("dedup_bam", "${MarkDups.dedup_bam}")
    .inputFile("dedup_bam_idx", "${MarkDups.dedup_bam_index}")
    .outputFile("recal_data", "${BwaMem.sample_name}.recal_data.table")
    .outputFile("post_recal_data", "${BwaMem.sample_name}.post_recal_data.table")
    .outputFile("bqsr_bam", "${BwaMem.sample_name}.recal_reads.bam")
    .preemptible(true)
    .diskSize(200)
    .memory(14)
    .cpu(4)
    .docker(JAVA8_IMAGE)
    .script(
      "set -o pipefail \n" +
      "gunzip ${dbSNP} \n" +
      "gunzip ${dbSNP_idx} \n" +
      "java -jar ${gatk_jar} -T BaseRecalibrator -R ${ref_fa} -I ${dedup_bam} \\\n" +
      "-knownSites dbsnp_138.b37.vcf -o ${recal_data} -nct 4 \n" +
      "java -jar ${gatk_jar} -T BaseRecalibrator -R ${ref_fa} -I ${dedup_bam} \\\n" +
      "-knownSites dbsnp_138.b37.vcf -BQSR ${recal_data} -o ${post_recal_data} -nct 4 \n" +
      "java -jar ${gatk_jar} -T PrintReads -R ${ref_fa} -I ${dedup_bam} \\\n" +
      "-BQSR ${recal_data} -o ${bqsr_bam} -nct 4 "   
    )
    .build();

  static Task HaplotypeCaller = TaskBuilder.named("HaplotypeCaller")
    .input("sample_name", "${BwaMem.sample_name}")
    .inputFile("gatk_jar", "gs://pipelines-api/software/gatk/GenomeAnalysisTK.jar")
    .inputFile("ref_fa", "gs://pipelines-api/ref-files/Homo-sapiens/b37/fasta/Homo_sapiens_assembly19.fasta")
    .inputFile("ref_fa_idx", "gs://pipelines-api/ref-files/Homo-sapiens/b37/fasta/Homo_sapiens_assembly19.fasta.fai")
    .inputFile("ref_fa_dict", "gs://pipelines-api/ref-files/Homo-sapiens/b37/fasta/Homo_sapiens_assembly19.dict")
    .inputFile("dbSNP", "gs://pipelines-api/ref-files/Homo-sapiens/b37/dbSNP/dbsnp_138.b37.vcf.gz")
    .inputFile("dbSNP_idx", "gs://pipelines-api/ref-files/Homo-sapiens/b37/dbSNP/dbsnp_138.b37.vcf.idx.gz")
    .inputFile("bqsr_bam", "${BQSR.bqsr_bam}")
    .outputFile("out_vcf", "${BwaMem.sample_name}.raw.snp_and_indel.vcf.gz")
    .outputFile("out_vcf_snp", "${BwaMem.sample_name}.raw.snps.vcf.gz")
    .outputFile("out_vcf_snp_filtered", "${BwaMem.sample_name}.filtered.snps.vcf.gz")
    .preemptible(true)
    .diskSize(200)
    .memory(14)
    .cpu(4)
    .docker(JAVA8_IMAGE)
    .script(
      "set -o pipefail \n" +
      "gunzip ${dbSNP} \n" +
      "gunzip ${dbSNP_idx} \n" +
      "java -jar ${gatk_jar} -T HaplotypeCaller -R ${ref_fa} -I ${bqsr_bam} \\\n" +
      "--dbsnp dbsnp_138.b37.vcf -o ${sample_name}.raw.snp_and_indel.vcf -nct 4 \n" +
      "java -jar ${gatk_jar} -T SelectVariants -R ${ref_fa} -V ${sample_name}.raw.snp_and_indel.vcf \\\n" +
      "-selectType SNP -o ${sample_name}.raw.snps.vcf -nt 4 \n" +
      "java -jar ${gatk_jar} -T VariantFiltration -R ${ref_fa} --variant ${sample_name}.raw.snps.vcf \\\n" +
      "-o ${sample_name}.filtered.snps.vcf -nt 4 --filterExpression 'QD < 2.0 || FS > 60.0 || MQ < 40.0 || MQRankSum < -12.5 || ReadPosRankSum < -8.0' --filterName 'synergist-default-snp-filter' \n" +
      "gzip ${sample_name}.raw.snp_and_indel.vcf \n" +
      "gzip ${sample_name}.raw.snps.vcf \n" +
      "gzip ${sample_name}.filtered.snps.vcf "
    )
    .build();

}
