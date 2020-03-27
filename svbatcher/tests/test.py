#!/usr/bin/env python

from unittest import TestCase
from svbatcher.batcher import SVBatcher
from svbatcher.data_types import Individual, Family
import svbatcher.tests.cohort_generator as generator
import constants as const
import os
import glob


class TestSVBatcher(TestCase):
    def setUp(self):
        generator.generate_and_write_files()
        if not os.path.exists(const.OUT_DIR_PATH):
            os.makedirs(const.OUT_DIR_PATH)

    def tearDown(self):
        generator.delete_files()
        out_glob_path = os.path.join(const.OUT_DIR_PATH, "batch.*.txt")
        out_files = list(glob.iglob(out_glob_path))
        for filepath in out_files:
            os.remove(filepath)
        os.rmdir(const.OUT_DIR_PATH)


    def integration_test_individuals(self):
        batcher = SVBatcher()
        cohort = batcher.load_cohort(const.COVERAGE_FILE_PATH, const.SEX_FILE_LIST_PATH, const.WGD_FILE_LIST_PATH)
        self.assertTrue(cohort)
        self.assertIsInstance(cohort, dict)
        self.assertEqual(len(cohort), const.TEST_COHORT_SIZE)
        for sample_id in cohort:
            self.assertIsInstance(sample_id, basestring)
            individual = cohort[sample_id]
            self.assertIsInstance(individual, Individual)
            self.assertIsNot(individual.coverage, None)
            self.assertIsNot(individual.wgd, None)
            self.assertIsNot(individual.sex, None)
            self.assertTrue(individual.is_female() or individual.is_male() or individual.is_sex_other())

        batches = batcher.batch_cohort(target_batch_size=const.TEST_TARGET_BATCH_SIZE,
                             num_coverage_quantiles=const.TEST_COVERAGE_QUANTILES,
                             verbosity=const.TEST_VERBOSITY)
        self.assertTrue(batches)
        self.assertIsInstance(batches, list)
        self.assertTrue(len(batches) >= const.TEST_MIN_ACCEPTABLE_BATCHES)
        self.assertTrue(len(batches) <= const.TEST_MAX_ACCEPTABLE_BATCHES)
        batch_sizes = [sum([y.size() for y in x]) for x in batches]
        self.assertTrue(min(batch_sizes) >= const.TEST_MIN_ACCEPTABLE_BATCH_SIZE)
        self.assertTrue(max(batch_sizes) <= const.TEST_MAX_ACCEPTABLE_BATCH_SIZE)

        batcher.write_output(const.OUT_DIR_PATH)
        out_glob_path = os.path.join(const.OUT_DIR_PATH, "batch.*.txt")
        print out_glob_path
        out_files = list(glob.iglob(out_glob_path))
        num_out_files = len(out_files)
        self.assertEqual(num_out_files, len(batches))
        for filepath in out_files:
            with open(filepath, 'r') as f:
                lines = f.readlines()
                self.assertTrue(lines)
                header = lines[0]
                self.assertTrue(Family.TABLE_HEADER_STRING in header)
                self.assertTrue(header.startswith('#'))
                num_individuals = len(lines) - 1
                self.assertTrue(num_individuals >= const.TEST_MIN_ACCEPTABLE_BATCH_SIZE)
                self.assertTrue(num_individuals <= const.TEST_MAX_ACCEPTABLE_BATCH_SIZE)
