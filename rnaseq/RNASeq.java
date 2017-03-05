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
  static final String TRIM_REPORT_IMAGE = "docker.io/mvangala/bioifx_preprocess_trimmomatic-plots:0.0.1";
  static final String STAR_REPORT_IMAGE = "docker.io/mvangala/bioifx_alignment_star-plots:0.0.1";
  static final String CUFF_IMAGE = "docker.io/mvangala/bioifx_alignment_cufflinks:0.0.1";
  static final String CUFF_MATRIX_IMAGE = STAR_REPORT_IMAGE; 
  static final String PCA_IMAGE = "docker.io/mvangala/bioifx_cluster_pca:0.0.1";

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
      .cpu(8)
      .docker(TRIM_IMAGE)
      .script(
        "set -o pipefail\n" +
	"TrimmomaticPE -threads 8 $leftmate $rightmate $leftmateP $leftmateU $rightmateP $rightmateU ILLUMINACLIP:/usr/share/trimmomatic/TruSeq2-PE.fa:2:30:10 LEADING:3 TRAILING:3 SLIDINGWINDOW:4:20 MINLEN:36 >&${logfile}"
       )
      .build();

  static Task TrimGather = TaskBuilder.named("TrimGather")
      .script("#do nothing")
      .input("pipelinerun", "${workflow.index}").gatherBy("pipelinerun")
      .build();
  
  static Task TrimReport = TaskBuilder.named("TrimReport")
      .inputFileArray("logfiles", " -l ", "${Trimmomatic.logfile}")
      .outputFile("trim_matrix", "trim_report.csv")
      .outputFile("trim_plot", "trim_report.png")
      .docker(TRIM_REPORT_IMAGE)
      .script(
        "set -euo pipefail\n" +
        "logfiles=\"-l ${logfiles} \"\n" +
        "matrix_command=\"perl /usr/local/bin/scripts/trim_report_pe.pl ${logfiles} 1> ${trim_matrix} \"\n" +
        "png_command=\"Rscript /usr/local/bin/scripts/trim_plot_pe.R ${trim_matrix} ${trim_plot} \"\n" +
        "echo \"Matrix Command: ${matrix_command} \"\n" +
        "echo \"PNG Command: ${png_command} \"\n" +
        "echo \"Log files: ${logfiles} \"\n" +
        " ${matrix_command} \n" +
        " ${png_command} "
       )
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
      //.outputFile("chi_junc", "${sample_name}.${chi_junc}")
      //.outputFile("chi_sam", "${sample_name}.${chi_sam}")
      .outputFile("gene_counts", "${sample_name}.${gene_counts}")
      //.outputFile("junc_bed", "${sample_name}.${junc_bed}")
      .outputFile("log_final", "${sample_name}.${log_final}")
      .outputFile("log_full", "${sample_name}.${log_full}")
      .outputFile("log_progress", "${sample_name}.${log_progress}")
      .outputFile("sj_out", "${sample_name}.${sj_out}")
      .outputFile("sorted_bam", "${sample_name}.${sorted_bam}")
      //End
      .preemptible(true)
      .diskSize("${agg_lg_disk}")
      .memory(60)
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
        " --outSAMtype BAM SortedByCoordinate \\\n" +
        " --limitBAMsortRAM 55000000000 --quantMode GeneCounts --outTmpDir /mnt/data/temp \\\n" +
        " && mv ${sample_name}.Aligned.sortedByCoord.out.bam ${sorted_bam} \\\n" +
        " && mv ${sample_name}.ReadsPerGene.out.tab ${gene_counts} \\\n" +
        //" && mv ${sample_name}.Chimeric.out.junction ${chi_junc} \\\n" +
        //" && mv ${sample_name}.Chimeric.out.sam ${chi_sam} \\\n" +
        //" && mv ${sample_name}.junctions.bed ${junc_bed} \\\n" +
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

  static Task STARReport = TaskBuilder.named("STARReport")
      .inputFileArray("gene_counts_list", " -f ", "${STAR.gene_counts}")
      .inputFileArray("log_list", " -l ", "${STAR.log_final}")
      .outputFile("log_csv", "STAR_Align_Report.csv")
      .outputFile("log_png", "STAR_Align_Report.png")
      .outputFile("gene_csv", "STAR_Gene_Counts.csv")
      .docker(STAR_REPORT_IMAGE)
      .script(
        "set -euo pipefail\n" +
        "perl /usr/local/bin/scripts/STAR_reports.pl -l ${log_list} -o $log_csv \n" +
        "Rscript /usr/local/bin/scripts/map_stats.R $log_csv $log_png \n" +
        "perl /usr/local/bin/scripts/raw_and_fpkm_count_matrix.pl -f ${gene_counts_list} -o $gene_csv "
      )
      .build();

  static Task Cufflinks = TaskBuilder.named("Cufflinks")
      .input("sample_name")
      .inputFile("sorted_bam", "${STAR.sorted_bam}")
      .inputFile("gtf_file", "${gtf_file}")
      .outputFile("genes_fpkm", "${sample_name}.genes.fpkm_tracking")
      .outputFile("iso_fpkm", "${sample_name}.isoforms.fpkm_tracking")
      .cpu(16)
      .memory(60)
      .diskSize("${agg_sm_disk}")
      .preemptible(true)
      .docker(CUFF_IMAGE)
      .script(
        "set -euo pipefail \n" +
        "cufflinks -p 16 -G $gtf_file $sorted_bam \n" +
        "mv genes.fpkm_tracking $genes_fpkm \n" +
        "mv isoforms.fpkm_tracking $iso_fpkm "
      )
      .build();

  static Task CuffGather = TaskBuilder.named("CuffGather")
      .script("#do nothing")
      .input("pipelinerun", "${workflow.index}").gatherBy("pipelinerun")
      .build();

  static Task CuffMatrix = TaskBuilder.named("CuffMatrix")
      .inputFileArray("gene_fpkm_list", " -f ", "${Cufflinks.genes_fpkm}")
      .outputFile("cuff_matrix", "Cuff_Gene_Counts.csv")
      .docker(CUFF_MATRIX_IMAGE)
      .script(
        "set -euo pipefail \n" +
        "perl /usr/local/bin/scripts/raw_and_fpkm_count_matrix.pl -c -d -f ${gene_fpkm_list} -o $cuff_matrix "
      )
      .build();

  static Task FilterCuff = TaskBuilder.named("FilterCuff")
      .input("in_matrix", "${CuffMatrix.cuff_matrix}")
      .inputArray("sample_names", " ", "${sample_name}")
      .outputFile("out_matrix", "Cuff_Gene_Counts.filtered.csv")
      .docker(PCA_IMAGE)
      .script(
        "set -euo pipefail \n" +
        "Rscript /usr/local/bin/scripts/filter_cuff_matrix.R \\\n" +
        "$in_matrix \"${sample_names}\" TRUE 1000 2 2.0 $out_matrix "
      )
      .build();

  static WorkflowArgs workflowArgs = ArgsBuilder.of()
      .input("Trimmomatic.sample_name", "${sample_name}")
      .input("STAR.sample_name", "${Trimmomatic.sample_name}")
      .input("Cufflinks.sample_name", "${Trimmomatic.sample_name}")
      .build();

  @Override
  public Workflow createWorkflow(String[] args) throws IOException {
    return TaskBuilder.named(RNASeq.class.getSimpleName())
      .steps(
        Steps.of(
          Trimmomatic,
          Branch.of(
            Steps.of(
              TrimGather,
              TrimReport
            ),
            Steps.of(
              STAR,
              Branch.of(
                Steps.of(
                  Cufflinks,
                  CuffGather,
                  CuffMatrix,
                  FilterCuff
                ),
                Steps.of(
                  STARGather,
                  STARReport
                )
              )
            )
          )
        )
      )
      .args(workflowArgs).build();
  }
}
