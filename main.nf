nextflow.enable.dsl=2

params.src_bucket = 'bmrn-external-engagements'
params.dst_bucket = 'r6333-pep-nppc-oi-bmn333-dev'
params.dst_prefix = 'humanized-isoseq'

// One entry per object still to move. Each runs as its own Batch task, so the
// three transfers happen in parallel and a single failure retries alone.
Channel.of(
  ['UPenn/P2010905/r84286_20260529_172905_1_C01/m84286_260529_214106_s1.hifi_reads.Barcode3.2010905_ARN-26-045_B1L1.bam', 'reads'],
  ['UPenn/P2010905/r84286_20260529_172905_1_C01/m84286_260529_214106_s1.hifi_reads.Barcode4.2010905_ARN-26-045_B1L2.bam', 'reads'],
  ['UPenn/SYNGAP1/TIGER_Outputs/ReferenceAssemblies/host_with_inserted_transgene.fasta', 'reference']
).set { objects }

process RELAY {
  tag   { src.tokenize('/')[-1] }
  errorStrategy 'retry'
  maxRetries 3
  cpus 4
  memory '8 GB'
  container 'quay.io/biocontainers/awscli:1.29.37'

  input:
  tuple val(src), val(subdir)

  output:
  path "relay_*.txt"

  script:
  def base = src.tokenize('/')[-1]
  """
  # Source account credentials come in as job env vars; the task role owns the destination.
  aws configure set aws_access_key_id     "\$SRC_KEY"    --profile src
  aws configure set aws_secret_access_key "\$SRC_SECRET" --profile src
  aws configure set aws_session_token     "\$SRC_TOKEN"  --profile src
  aws configure set region us-west-2 --profile src
  aws configure set default.s3.max_concurrent_requests 32
  aws configure set default.s3.multipart_chunksize 64MB

  SZ=\$(aws s3api head-object --profile src \
         --bucket ${params.src_bucket} --key "${src}" \
         --query ContentLength --output text)
  echo "source size: \$SZ" > relay_${base}.txt

  # Intra-region stream: read with the source profile, write with the task role.
  aws s3 cp --profile src --quiet "s3://${params.src_bucket}/${src}" - \
    | aws s3 cp --quiet --expected-size "\$SZ" - \
      "s3://${params.dst_bucket}/${params.dst_prefix}/${subdir}/${base}"

  DST=\$(aws s3api head-object --bucket ${params.dst_bucket} \
          --key "${params.dst_prefix}/${subdir}/${base}" \
          --query ContentLength --output text)
  echo "dest size:   \$DST" >> relay_${base}.txt
  # Byte-for-byte size check: a truncated stream fails the task rather than passing silently.
  if [ "\$SZ" != "\$DST" ]; then echo "SIZE MISMATCH" >> relay_${base}.txt; exit 1; fi
  echo "OK ${base}" >> relay_${base}.txt
  """
}

workflow { RELAY(objects) }