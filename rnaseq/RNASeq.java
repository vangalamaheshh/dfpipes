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
  static final String STAR_IMAGE = "docker.io/mvangala/bioifx_alignment_star:0.0.1";

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
      .diskSize("${agg_sm_disk}")
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
      .input("sample_name")
      .input("genome_dir", "/mnt/data")
      .inputFile("leftmate", "${Trimmomatic.leftmateP}")
      .inputFile("rightmate", "${Trimmomatic.rightmateP}")
      //STAR reference files
      .inputFile("chr_len", "${chr_len}")
      .inputFile("chr_name", "${chr_name}")
      .inputFile("chr_name_len", "${chr_name_len}")
      .inputFile("chr_start", "${chr_start}")
      .inputFile("exon_gene_info", "${exon_gene_info}")
      .inputFile("exon_info", "${exon_info}")
      .inputFile("gene_info", "${gene_info}")
      .inputFile("genome_info", "${genome_info}")
      .inputFile("genome_params", "${genome_params}")
      .inputFile("sa_info", "${sa_info}")
      .inputFile("sa_index", "${sa_index}")
      .inputFile("sjdb_info", "${sjdb_info}")
      .inputFile("sjdb_gtf", "${sjdb_gtf}")
      .inputFile("sjdb_list", "${sjdb_list}")
      .inputFile("trans_info", "${trans_info}")
      .inputFile("gtf_file", "${gtf_file}")
      //STAR out files
      .outputFile("chi_junc", "${sample_name}.${chi_junc}")
      .outputFile("chi_sam", "${sample_name}.${chi_sam}")
      .outputFile("gene_counts", "${sample_name}.${gene_counts}")
      .outputFile("junc_bed", "${sample_name}.${junc_bed}")
      .outputFile("log_final", "${sample_name}.${log_final}")
      .outputFile("log_full", "${sample_name}.${log_full}")
      .outputFile("log_progress", "${sample_name}.${log_progress}")
      .outputFile("sj_out", "${sample_name}.${sj_out}")
      .outputFile("unsorted_bam", "${sample_name}.${unsorted_bam}")
      //End
      .preemptible(true)
      .diskSize("${agg_lg_disk}")
      .memory(45)
      .cpu(16)
      .docker(STAR_IMAGE)
      .script(
        "set -o pipefail\n" +
        "for cur_file in $(find /mnt/data/ -type f); do file_name=$(basename $cur_file); if [[ $file_name =~ ^[0-9] ]]; then out_file=$(echo $file_name | sed -E \"s/^[0-9]+-//g\"); ln -s /mnt/data/$file_name /mnt/data/$out_file; fi; done" + "\n" +
        "STAR --runMode alignReads --runThreadN 16 --genomeDir $genome_dir \\\n" +
        " --sjdbGTFfile $gtf_file \\\n" +
        " --readFilesIn $leftmate $rightmate --readFilesCommand zcat \\\n" + 
        " --outFileNamePrefix $sample_name. \\\n" +
        " --outSAMstrandField intronMotif \\\n" +
        " --outSAMmode Full --outSAMattributes All \\\n" +
        " --outSAMattrRGline ID:${sample_name} PL:illumina LB:${sample_name} SM:${sample_name} \\\n" + 
        " --outSAMtype BAM Unsorted \\\n" +
        " --quantMode GeneCounts \\\n" +
        " && mv ${sample_name}.Aligned.out.bam ${unsorted_bam} \\\n" +
        " && mv ${sample_name}.ReadsPerGene.out.tab ${gene_counts} \\\n" +
        " && mv ${sample_name}.Chimeric.out.junction ${chi_junc} \\\n" +
        " && mv ${sample_name}.Chimeric.out.sam ${chi_sam} \\\n" +
        " && mv ${sample_name}.junctions.bed ${junc_bed} \\\n" +
        " && mv ${sample_name}.Log.final.out ${log_final} \\\n" +
        " && mv ${sample_name}.Log.out ${log_full} \\\n" +
        " && mv ${sample_name}.Log.progress.out ${log_progress} \\\n" +
        " && mv ${sample_name}.SJ.out.tab ${sj_out} \\\n"
      )
      .build();

  static Task STARGather = TaskBuilder.named("STARGather")
      .script("#do nothing")
      .input("pipelinerun", "${workflow.index}").gatherBy("pipelinerun")
      .build();
	
  static WorkflowArgs workflowArgs = ArgsBuilder.of()
      .input("Trimmomatic.sample_name", "${sample_name}")
      .input("STAR.sample_name", "${Trimmomatic.sample_name}")
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
