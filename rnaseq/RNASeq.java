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
import com.google.cloud.genomics.dockerflow.task.TaskDefn;
import com.google.cloud.genomics.dockerflow.task.TaskDefn.Param;

public class RNASeq implements WorkflowDefn {
  static final String TRIM_IMAGE = "docker.io/mvangala/bioifx_preprocess_trimmomatic:0.0.1";
  static final String STAR_IMAGE = "docker.io/mvangala/bioifx_alignment_star:0.0.1";
  static final String TRIM_REPORT_IMAGE = "docker.io/mvangala/bioifx_preprocess_trimmomatic-plots:0.0.1";
  static final String STAR_REPORT_IMAGE = "docker.io/mvangala/bioifx_alignment_star-plots:0.0.1";
  static final String CUFF_IMAGE = "docker.io/mvangala/bioifx_alignment_cufflinks:0.0.1";
  static final String CUFF_MATRIX_IMAGE = STAR_REPORT_IMAGE; 
  static final String PCA_IMAGE = "docker.io/mvangala/bioifx_cluster_pca:0.0.1";
  static final String HEATMAP_IMAGE = "docker.io/mvangala/bioifx_cluster_heatmap:0.0.1";
  static final String DE_IMAGE = "docker.io/mvangala/bioifx_diff-exp_deseq2:0.0.1";

  static Task Trimmomatic = TaskBuilder.named("Trimmomatic")
      .input("sample_name").scatterBy("sample_name")
      .inputFile("leftmate", "gs://bib_somerandomword/input/rnaseq/fastq/ex/${sample_name}_R1.fastq.gz")
      .inputFile("rightmate", "gs://bib_somerandomword/input/rnaseq/fastq/ex/${sample_name}_R2.fastq.gz")
      .outputFile("leftmateP", "${sample_name}.left.paired.trim.fastq.gz")
      .outputFile("leftmateU", "${sample_name}.left.unpaired.trim.fastq.gz")
      .outputFile("rightmateP", "${sample_name}.right.paired.trim.fastq.gz")
      .outputFile("rightmateU", "${sample_name}.right.unpaired.trim.fastq.gz")
      .outputFile("logfile", "${sample_name}.trim.log")
      .preemptible(true)
      .diskSize("${agg_sm_disk}")
      .memory(4)
      .cpu(6)
      .docker(TRIM_IMAGE)
      .script(
        "set -o pipefail\n" +
	"TrimmomaticPE -threads 6 $leftmate $rightmate $leftmateP $leftmateU $rightmateP $rightmateU ILLUMINACLIP:/usr/share/trimmomatic/TruSeq2-PE.fa:2:30:10 LEADING:3 TRAILING:3 SLIDINGWINDOW:4:20 MINLEN:36 >&${logfile}"
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

/*
  static Task STAR = TaskBuilder.named("STAR")
      .input("sample_name")
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
      .outputFile("gene_counts", "${sample_name}.${gene_counts}")
      .outputFile("log_final", "${sample_name}.${log_final}")
      .outputFile("log_full", "${sample_name}.${log_full}")
      .outputFile("log_progress", "${sample_name}.${log_progress}")
      .outputFile("sj_out", "${sample_name}.${sj_out}")
      .outputFile("sorted_bam", "${sample_name}.${sorted_bam}")
      //End
      .preemptible(true)
      .diskSize("${agg_lg_disk}")
      .memory(45)
      .cpu(6)
      .docker(STAR_IMAGE)
      .script(
        "set -o pipefail\n" +
        "for cur_file in $(find /mnt/data/ -type f); do file_name=$(basename $cur_file); if [[ $file_name =~ ^[0-9] ]]; then out_file=$(echo $file_name | sed -E \"s/^[0-9]+-//g\"); ln -s $file_name $out_file; fi; done" + "\n" +
        "STAR --runMode alignReads --runThreadN 6 --genomeDir $PWD \\\n" +
        " --sjdbGTFfile $gtf_file \\\n" +
        " --readFilesIn $leftmate $rightmate --readFilesCommand zcat \\\n" + 
        " --outFileNamePrefix $sample_name. \\\n" +
        " --outSAMstrandField intronMotif \\\n" +
        " --outSAMmode Full --outSAMattributes All \\\n" +
        " --outSAMattrRGline ID:${sample_name} PL:illumina LB:${sample_name} SM:${sample_name} \\\n" + 
        " --outSAMtype BAM SortedByCoordinate \\\n" +
        " --limitBAMsortRAM 45000000000 --quantMode GeneCounts --outTmpDir $PWD/temp \\\n" +
        " && mv ${sample_name}.Aligned.sortedByCoord.out.bam ${sorted_bam} \\\n" +
        " && mv ${sample_name}.ReadsPerGene.out.tab ${gene_counts} \\\n" +
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
      .inputFile("in_matrix", "${CuffMatrix.cuff_matrix}")
      .outputFile("out_matrix", "Cuff_Gene_Counts.filtered.csv")
      .docker(PCA_IMAGE)
      .script(
        "set -euo pipefail \n" +
        "command=\"Rscript /usr/local/bin/scripts/filter_cuff_matrix.R $in_matrix TRUE 1000 2 2.0 $out_matrix \"\n" +
        "echo $command \n" +
        "$command"
      )
      .build();

  static Task PCA = TaskBuilder.named("PCA")
      .inputArray("metadata", "\n", "${metadata}")
      .inputFile("rpkm_file", "${FilterCuff.out_matrix}")
      .outputFile("pca_out_pdf", "pca.pdf")
      .input("pca_out_dir", "pca_images")
      .input("gs_bucket", "${gs_bucket}/PCA")
      .diskSize("${agg_sm_disk}")
      .docker(PCA_IMAGE)
      .script(
        "set -euo pipefail\n" +
        "printf \"${metadata}\" | perl -e 'my $file = \"metasheet.csv\"; open(OFH, \">$file\"); while(my $line = <STDIN>) { print OFH $line; } close OFH;'\n" +
        "cat metasheet.csv\n" +
        "mkdir -p $pca_out_dir\n" +
        "Rscript /usr/local/bin/scripts/pca_plot.R $rpkm_file metasheet.csv $pca_out_pdf $pca_out_dir \n" +
        "gsutil cp -r $pca_out_dir $gs_bucket " 
      )
      .build();
 

  static Task Heatmap = TaskBuilder.named("Heatmap")
      .inputArray("metadata", "\n", "${metadata}")
      .inputFile("rpkm_file", "${FilterCuff.out_matrix}")
      .input("cluster", "${heatmap_cluster}")
      .input("gs_bucket", "${gs_bucket}/Heatmap")
      .diskSize("${agg_sm_disk}")
      .docker(HEATMAP_IMAGE)
      .script(
        "set -euo pipefail\n" +
        "printf \"${metadata}\" | perl -e 'my $file = \"metasheet.csv\"; open(OFH, \">$file\"); while(my $line = <STDIN>) { print OFH $line; } close OFH;'\n" +
        "cat metasheet.csv\n" +
        "mkdir -p SS SF\n" +
        "Rscript /usr/local/bin/scripts/heatmapSF.R $rpkm_file metasheet.csv \"$cluster\" SF \n" +
        "Rscript /usr/local/bin/scripts/heatmapSS.R $rpkm_file metasheet.csv SS \n" + 
        "gsutil cp -r SS/* $gs_bucket/SS \n" +
        "gsutil cp -r SF/* $gs_bucket/SF "
      )
      .build();

  static Task DE = TaskBuilder.named("DE")
      .inputArray("metadata", "\n", "${metadata}")
      .inputFile("gene_counts", "${STARReport.gene_csv}")
      .input("gs_bucket", "${gs_bucket}/DE")
      .diskSize("${agg_sm_disk}")
      .docker(DE_IMAGE)
      .script(
        "set -euo pipefail\n" +
        "printf \"${metadata}\" | perl -e 'my $file = \"metasheet.csv\"; open(OFH, \">$file\"); while(my $line = <STDIN>) { print OFH $line; } close OFH;'\n" +
        "cat metasheet.csv\n" +
        "mkdir -p $(head -1 metasheet.csv | perl -e 'my $line = <STDIN>; chomp $line; $line =~ s/.+?comp_/comp_/; $line =~ s/,/ /g; print $line;')\n" +
        "Rscript /usr/local/bin/scripts/DE.R metasheet.csv $gene_counts \n" +
        "gsutil cp -r comp* $gs_bucket "       
      )
      .build();

 static Task PATH = TaskBuilder.named("PATH")
      .script("#do nothing")
      .build(); 

*/
  static WorkflowArgs workflowArgs = ArgsBuilder.of()
      .input("Trimmomatic.sample_name", "${sample_name}")
//      .input("STAR.sample_name", "${Trimmomatic.sample_name}")
//      .input("Cufflinks.sample_name", "${Trimmomatic.sample_name}")
      .build();

  @Override
  public Workflow createWorkflow(String[] args) throws IOException {
    return TaskBuilder.named(RNASeq.class.getSimpleName())
      .steps(
        Steps.of(
          Trimmomatic,
          TrimGather,
          TrimReport
/*          Branch.of(
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
                  FilterCuff,
                  Branch.of(
                    PCA,
                    Heatmap
                  )
                ),
                Steps.of(
                  STARGather,
                  STARReport,
                  Branch.of(
                    DE,
                    PATH
                  )
                )
              )
            )
          )
*/        )
      )
      .args(workflowArgs).build();
  }
}
