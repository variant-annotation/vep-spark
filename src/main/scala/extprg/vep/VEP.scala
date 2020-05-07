package extprg.vep

import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import OutputHandler.RDDExtensions

object VEP {
  def main(args: Array[String]) {
    val spark = SparkSession.builder()
      .appName("Simple Application")
      .master("yarn")
      .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000")
      .config("spark.yarn.jars", "hdfs://172.16.203.128:9000/user/ntqh/jars/*.jar")
      .config("spark.hadoop.yarn.resourcemanager.address", "172.16.203.128:8032")
      .config("spark.hadoop.yarn.application.classpath", "$HADOOP_HOME,$HADOOP_CONF_DIR,$HADOOP_COMMON_HOME/*,$HADOOP_COMMON_HOME/lib/*,$HADOOP_HDFS_HOME/*,$HADOOP_HDFS_HOME/lib/*,$HADOOP_MAPRED_HOME/*,$HADOOP_MAPRED_HOME/lib/*,$HADOOP_YARN_HOME/*,$HADOOP_YARN_HOME/lib/*")
      .getOrCreate()
    val dataRDD = spark.sparkContext.textFile("/hdfs://namenode:9000/user/ntqh/data/vcf_samples/sample1.vcf")
    val cmd = "/home/ensembl-vep/vep --format vcf --no_stats --force_overwrite --cache --offline --vcf --vcf_info_field ANN --buffer_size 60000 --phased --hgvsg --hgvs --symbol --variant_class --biotype --gene_phenotype --regulatory --ccds --transcript_version --tsl --appris --canonical --protein --uniprot --domains --sift b --polyphen b --check_existing --af --max_af --af_1kg --af_gnomad --minimal --allele_number --pubmed --fasta /home/.vep/homo_sapiens/99_GRCh38/Homo_sapiens.GRCh38.dna.toplevel.fa.gz -o STDOUT"
    val pipeRDD = dataRDD.pipe(cmd)
    pipeRDD.saveAsSingleTextFile("hdfs://namenode:9000/user/ntqh/output/")
  }
}