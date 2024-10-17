#!/bin/bash
#SBATCH --job-name=tdb_dataset
#SBATCH --output=/scratch/hpc-prf-lola/albert/get_props/logs/process_wikidata.out
#SBATCH --error=/scratch/hpc-prf-lola/albert/get_props/logs/process_wikidata.err
#SBATCH --time=504:00:00
#SBATCH --partition=largemem
#SBATCH --nodes=1
#SBATCH --ntasks=1
#SBATCH --cpus-per-task=1
#SBATCH --mem=128G
#SBATCH --mail-type=END,FAIL
#SBATCH --mail-user=akhomich@mail.uni-paderborn.de

module load lang/Java/21.0.1

export JAVA_TOOL_OPTIONS="-Xmx120G"

tdbloader --loc=/scratch/hpc-prf-lola/albert/get_props/data/tdb_dataset /scratch/hpc-prf-lola/albert/get_props/data/latest-all.ttl

tdbquery --loc=/scratch/hpc-prf-lola/albert/get_props/data/tdb_dataset --query=/scratch/hpc-prf-lola/albert/get_props/data/extract_properties_all_labels.rq --result=nt > /scratch/hpc-prf-lola/albert/get_props/data/wikidata_properties.nt