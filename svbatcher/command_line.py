#!/usr/bin/env python

import argparse
from svbatcher.batcher import SVBatcher


def main():
    usage = """Divides a cohort into sex-balanced batches clustered by proband coverage and dosage score.
       Batches are written to individual tsv files in the given output directory."""
    parser = argparse.ArgumentParser(usage=usage)
    parser.add_argument("cohort_file_path", help="File containing coverage data with columns: sample_id, coverage. This sample list is used to define the cohort.")
    parser.add_argument("sex_assignment_file_list", help="File containing a list of sex assignment file paths with columns: sample_id, anything, anything, sex(MALE/FEMALE/OTHER)")
    parser.add_argument("wgd_file_list", help="File containing a list of WGD score file paths with columns: sample_id, wgd_score")
    parser.add_argument("output_dir", help="Output directory")
    parser.add_argument("--ped", help="Family ped file. If not provided, each sample is treated as a proband in a single-individual family.")
    parser.add_argument("--batch_size", help="Desired batch size (default = 200)", type=int, default=200)
    parser.add_argument("--coverage_quantiles", help="Number of coverage quantiles (default = max[N/(4*batch_size), 1], where N is the total cohort size)", type=int)
    parser.add_argument("--min_sex_count", help="Minimum count of males and females per batch", type=int, default=50)
    parser.add_argument("--sex_balancing", help="Attempt to balance batch sex counts (0 = disabled, 1 = enabled, results in poorer metric clustering)", type=int, default=False)
    parser.add_argument("--verbosity", help="0 = none, 1 = write parameters/stats", type=int, default=1)
    args = parser.parse_args()

    batcher = SVBatcher()
    batcher.load_cohort(args.cohort_file_path, args.sex_assignment_file_list, args.wgd_file_list)
    batcher.batch_cohort(target_batch_size=args.batch_size,
                         num_coverage_quantiles=args.coverage_quantiles,
                         min_sex_count=args.min_sex_count,
                         use_sex_balancing=args.sex_balancing,
                         ped_file_path=args.ped,
                         verbosity=args.verbosity)
    batcher.write_output(args.output_dir)


if __name__ == "__main__":
    main()
