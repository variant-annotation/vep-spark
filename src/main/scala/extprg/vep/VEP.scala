package extprg.vep

import org.apache.spark.sql.SparkSession
import OutputHandler.RDDExtensions

object VEP {
  def main(args: Array[String]) {
    val spark = SparkSession.builder()
      .appName("VEPSpark Application")
      .master("yarn")
      .getOrCreate()
    // Arguments from cli
    val input_path = "/input/sample.vcf"
    val output_path = "/output/annotated_sample.vcf"
    val number_partitions = 500
    val cmd = "/home/ensembl-vep/vep --format vcf --no_stats --force_overwrite --cache --offline --vcf --vcf_info_field ANN --buffer_size 60000 --phased --hgvsg --hgvs --symbol --variant_class --biotype --gene_phenotype --regulatory --ccds --transcript_version --tsl --appris --canonical --protein --uniprot --domains --sift b --polyphen b --check_existing --af --max_af --af_1kg --af_gnomad --minimal --allele_number --pubmed --fasta /home/.vep/homo_sapiens/99_GRCh38/Homo_sapiens.GRCh38.dna.toplevel.fa.gz -o STDOUT"
    //    val cmd = "/home/spark/ensembl-vep/vep --format vcf --no_stats --force_overwrite --cache --offline --vcf --vcf_info_field ANN --buffer_size 1000 --phased --hgvsg --hgvs --symbol --variant_class --biotype --gene_phenotype --regulatory --ccds --transcript_version --tsl --appris --canonical --protein --uniprot --domains --sift b --polyphen b --check_existing --af --max_af --af_1kg --af_gnomad --minimal --allele_number --pubmed --fasta /home/spark/.vep/homo_sapiens/95_GRCh38/Homo_sapiens.GRCh38.dna_sm.primary_assembly.fa.gz -o STDOUT"
    val dataRDD = spark.sparkContext.textFile(input_path, minPartitions = number_partitions)
    val pipeRDD = dataRDD.pipe(cmd)
    pipeRDD.saveAsSingleTextFile(output_path)
  }
}