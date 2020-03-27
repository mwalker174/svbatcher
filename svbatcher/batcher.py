#!/usr/bin/env python

######################################################
#
# Cohort batching module for the GATK SV pipeline
#                 "It's batchin'!"
# written by Mark Walker (markw@broadinstitute.org)
#
######################################################

import sys
from svbatcher.utils import io, printers
from svbatcher.data_types import Individual, Family
import numpy as np

# Default parameters
DEFAULT_BATCH_SIZE = 200
DEFAULT_MIN_SEX_COUNT = 50
DEFAULT_VERBOSITY = 1
DEFAULT_SEX_BALANCED = 0

# Constraints
MIN_COVERAGE_QUANTILES = 1
MIN_TARGET_BATCH_SIZE = 1


class SVBatcher:
    def __init__(self):
        self.individuals_dict = None
        self.families = None
        self.batches = None

    def _set_all_proband(self):
        for sample_id in self.individuals_dict:
            self.individuals_dict[sample_id].proband = True

    def _check_families(self):
        for family in self.families:
            for ind in family.members:
                if not ind.is_fully_defined():
                    printers.raise_error("Individual not fully defined: " + str(ind))

    def _wrap_individuals(self):
        families = []
        family_id = 0
        for sample_id in self.individuals_dict:
            families.append(Family(family_id, [self.individuals_dict[sample_id]]))
            family_id += 1
        return families

    def _split_batch_by_categorizing(self, batch, category_func):
        values = list(set([category_func(x) for x in batch]))
        split_batch_dict = {}
        for val in values:
            split_batch_dict[val] = []
        for item in batch:
            val = category_func(item)
            split_batch_dict[val].append(item)
        return zip(*split_batch_dict.items())

    def _create_empty_split_batch(self, size):
        batch = []
        for i in range(size):
            batch.append([])
        return batch

    def _split_batch_by_sorting(self, batch, num_splits, value_func, item_size_func):
        split_batch = self._create_empty_split_batch(num_splits)
        sorted_batch = sorted(batch, key=value_func)
        batch_size = sum([item_size_func(x) for x in batch])
        counter = 0
        for sorted_item in sorted_batch:
            batch_idx = int((counter / float(batch_size)) * num_splits)
            split_batch[batch_idx].append(sorted_item)
            counter += item_size_func(sorted_item)
        return split_batch

    def _merge_batches(self, batches):
        batch = []
        for b in batches:
            batch.extend(b)
        return batch

    # Define "sex" for families
    def _family_sex(self, family):
        return family.proband.sex_char()

    # Define "coverage" for families
    def _family_coverage(self, family):
        return family.proband.coverage

    # Define "dosage score" for families
    def _family_dosage_score(self, family):
        return family.proband.wgd

    # Define "size" for families, which affects batch size balancing
    def _family_size(self, family):
        return family.size()

    def _batch_families_not_sex_balanced(self, num_coverage_batches, num_wgd_batches):
        # Split into quantiles based on coverage
        families_by_coverage = self._split_batch_by_sorting(self.families, num_coverage_batches, self._family_coverage, self._family_size)

        # Split by WGD
        families_by_wgd = []
        for cov_batch in families_by_coverage:
            wgd_batch = self._split_batch_by_sorting(cov_batch, num_wgd_batches, self._family_dosage_score, self._family_size)
            families_by_wgd.append(wgd_batch)

        # Flatten into final batch list
        final_batches = []
        for cov_batch in families_by_wgd:
            for wgd_batch in cov_batch:
                final_batches.append(wgd_batch)
        return final_batches

    def _batch_families_sex_balanced(self, num_coverage_batches, num_wgd_batches):
        #Split first by sex
        sexes, families_by_sex = self._split_batch_by_categorizing(self.families, self._family_sex)

        # Split into quantiles based on coverage
        families_by_coverage = []
        for sex_batch in families_by_sex:
            cov_batches = self._split_batch_by_sorting(sex_batch, num_coverage_batches, self._family_coverage, self._family_size)
            families_by_coverage.append(cov_batches)

        # Split by WGD
        families_by_wgd = []
        for sex_batch in families_by_coverage:
            families_by_wgd.append([])
            for cov_batch in sex_batch:
                wgd_batch = self._split_batch_by_sorting(cov_batch, num_wgd_batches, self._family_dosage_score, self._family_size)
                families_by_wgd[-1].append(wgd_batch)

        # Merge sexes and flatten into final batch list
        final_batches = []
        num_sexes = len(sexes)
        for cov_idx in range(num_coverage_batches):
            for wgd_idx in range(num_wgd_batches):
                merged_batch = self._merge_batches([families_by_wgd[i][cov_idx][wgd_idx] for i in range(num_sexes)])
                final_batches.append(merged_batch)
        return final_batches

    def load_cohort(self, cohort_path, sex_assignment_list_path, wgd_list_path):
        self.individuals_dict = io.read_coverage_file(cohort_path)
        self.individuals_dict = io.read_sex_assignment_list(sex_assignment_list_path, self.individuals_dict)
        self.individuals_dict = io.read_wgd_list(wgd_list_path, self.individuals_dict)
        return self.individuals_dict

    def _get_array_stats(self, array):
        return [np.mean(array), np.std(array), np.min(array), np.max(array)]

    def _get_mean_batch_metrics_by_sex(self, batches, metric_func):
        metrics = []
        for batch in batches:
            metrics_male = [metric_func(x) for x in batch if x.proband.is_male()]
            metrics_female = [metric_func(x) for x in batch if x.proband.is_female()]
            metrics_other = [metric_func(x) for x in batch if x.proband.is_sex_other()]
            metrics.append([None, None, None])
            if metrics_male:
                metrics[-1][0] = np.mean(metrics_male)
            if metrics_female:
                metrics[-1][1] = np.mean(metrics_female)
            if metrics_other:
                metrics[-1][2] = np.mean(metrics_other)
        return metrics

    def batch_cohort(self,
                     target_batch_size=DEFAULT_BATCH_SIZE,
                     num_coverage_quantiles=None,
                     min_sex_count=DEFAULT_MIN_SEX_COUNT,
                     use_sex_balancing=DEFAULT_SEX_BALANCED,
                     ped_file_path=None,
                     verbosity=DEFAULT_VERBOSITY):
        # Set number of quantiles
        cohort_size = len(self.individuals_dict)
        if num_coverage_quantiles is None:
            num_coverage_quantiles = max(int(cohort_size/float(4*target_batch_size)), MIN_COVERAGE_QUANTILES)
        elif num_coverage_quantiles < MIN_COVERAGE_QUANTILES:
            printers.raise_error("Number of coverage quantiles must be >= " + str(MIN_COVERAGE_QUANTILES))

        if target_batch_size < MIN_TARGET_BATCH_SIZE:
            printers.raise_error("Target batch size must be >= " + str(MIN_TARGET_BATCH_SIZE))

        if verbosity:
            sys.stderr.write("################ Parameters ################\n")
            printers.print_parameter("Target batch size", target_batch_size)
            printers.print_parameter("Coverage quantiles", num_coverage_quantiles)
            printers.print_parameter("Cohort size", cohort_size)
            printers.print_parameter("Sex balancing", use_sex_balancing)

        # If ped file not provided, put each individual into a singleton family as a proband
        if not ped_file_path:
            self._set_all_proband()
            self.families = self._wrap_individuals()
        else:
            self.families = io.assign_families(ped_file_path, self.individuals_dict)

        # Input consistency check
        self._check_families()

        # Compute number of wgd batches
        cohort_size = sum([self._family_size(x) for x in self.families])
        num_wgd_batches = int((cohort_size / num_coverage_quantiles) / target_batch_size)

        # Run batching
        if use_sex_balancing:
            printers.print_warning("Sex balancing may result in poorer metric clustering.")
            batches = self._batch_families_sex_balanced(num_coverage_quantiles, num_wgd_batches)
        else:
            batches = self._batch_families_not_sex_balanced(num_coverage_quantiles, num_wgd_batches)

        # Stats
        num_batches = len(batches)
        batch_sizes = [sum([x.size() for x in y]) for y in batches]
        batched_cohort_size = sum(batch_sizes)
        batch_sizes_male = [sum([x.num_male() for x in y]) for y in batches]
        batch_sizes_female = [sum([x.num_female() for x in y]) for y in batches]
        batch_sizes_sex_other = [sum([x.num_sex_other() for x in y]) for y in batches]
        batch_sex_ratios = [batch_sizes_female[i] / float(batch_sizes_male[i]) for i in range(num_batches)]
        batch_coverage = self._get_mean_batch_metrics_by_sex(batches, self._family_coverage)
        batch_wgd = self._get_mean_batch_metrics_by_sex(batches, self._family_dosage_score)

        if verbosity:
            sys.stderr.write("################ Results ################\n")
            printers.print_parameter("Batches", num_batches)
            printers.print_parameter("Batched cohort size", batched_cohort_size)
            printers.print_parameter("Batch size (mean/std/min/max)", self._get_array_stats(batch_sizes))
            printers.print_parameter("Batch males (mean/std/min/max)",self. _get_array_stats(batch_sizes_male))
            printers.print_parameter("Batch females (mean/std/min/max)", self._get_array_stats(batch_sizes_female))
            printers.print_parameter("Batch other sex (mean/std/min/max)", self._get_array_stats(batch_sizes_sex_other))
            printers.print_parameter("Batch F/M ratio (mean/std/min/max)", self._get_array_stats(batch_sex_ratios))
            for cov in batch_coverage:
                printers.print_parameter("Mean coverage (male/female/other)", cov)
            for wgd in batch_wgd:
                printers.print_parameter("Mean dosage score (male/female/other)", wgd)

        # Enforce min_sex_count
        min_male_count = np.min(batch_sizes_male)
        min_female_count = np.min(batch_sizes_female)
        if min_male_count < min_sex_count or min_female_count < min_sex_count:
            printers.raise_error("At least one batch had less than minimum number of (fe)males (" + str(min_sex_count) + "). Try enabling sex balancing or lowering the minimum count.")

        # Bug check - this should never happen
        if batched_cohort_size != cohort_size:
            printers.raise_error("!!!!!!!! Final batched cohort size does not equal the input cohort size !!!!!!!!")

        self.batches = batches
        return batches

    def write_output(self, output_dir):
        io.write_output(self.batches, output_dir)
