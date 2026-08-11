nextflow.enable.dsl=2

// Relay PacBio Iso-Seq inputs between two AWS accounts, in-region (us-west-2).
// Nextflow's own S3 client stages the source object and publishes the copy, so
// no AWS CLI is needed in the container and no source credentials are passed in
// as env vars -- the source account's keys are supplied to the head job as
// aws.client credentials for the SOURCE bucket only.

params.dst = 's3://r6333-pep-nppc-oi-bmn333-dev/humanized-isoseq'

Channel.of(
  ['s3://bmrn-external-engagements/UPenn/P2010905/r84286_20260529_172905_1_C01/m84286_260529_214106_s1.hifi_reads.Barcode3.2010905_ARN-26-045_B1L1.bam', 'reads'],
  ['s3://bmrn-external-engagements/UPenn/P2010905/r84286_20260529_172905_1_C01/m84286_260529_214106_s1.hifi_reads.Barcode4.2010905_ARN-26-045_B1L2.bam', 'reads'],
  ['s3://bmrn-external-engagements/UPenn/SYNGAP1/TIGER_Outputs/ReferenceAssemblies/host_with_inserted_transgene.fasta', 'reference']
).map { uri, sub -> tuple(file(uri), sub) }.set { objects }

process RELAY {
  tag { src.name }
  errorStrategy 'retry'
  maxRetries 3
  cpus 4
  memory '8 GB'
  container 'quay.io/biocontainers/star:2.7.10b--h6b7c446_1'
  publishDir { "${params.dst}/${sub}" }, mode: 'copy', overwrite: true

  input:
  tuple path(src), val(sub)

  output:
  path "${src.name}"

  script:
  """
  # Fusion has already mounted the source object; verify it is non-empty and
  # fully staged before publishDir copies it to the destination bucket.
  test -s "${src}" || { echo "empty or missing source" >&2; exit 1; }
  ls -l "${src}"
  """
}

workflow { RELAY(objects) }