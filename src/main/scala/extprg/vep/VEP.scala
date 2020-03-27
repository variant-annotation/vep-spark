package extprg.vep

import org.apache.spark.{SparkConf, SparkContext}

object VEP {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local")
    val sc = new SparkContext(conf)
    val dataRDD = sc.textFile("/media/steve/DATA/vcf_testing_files/small.vcf")
    val cmd = "/home/steve/Downloads/ensembl-vep/vep --format vcf --no_stats --force_overwrite --cache --offline --vcf --vcf_info_field ANN --buffer_size 60000 --phased --hgvsg --hgvs --symbol --variant_class --biotype --gene_phenotype --regulatory --ccds --transcript_version --tsl --appris --canonical --protein --uniprot --domains --sift b --polyphen b --check_existing --af --max_af --af_1kg --af_gnomad --minimal --allele_number --pubmed --fasta /home/steve/.vep/homo_sapiens/95_GRCh38/Homo_sapiens.GRCh38.dna_sm.primary_assembly.fa.bgz -o STDOUT"
    val pipeRDD = dataRDD.pipe(cmd)
    pipeRDD.collect().foreach(println)
  }
}