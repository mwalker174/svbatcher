#!/usr/bin/env python

TEST_COHORT_SIZE = 10000
WGD_FILE_LIST_PATH = "./svbatcher/tests/test_wgd.list"
SEX_FILE_LIST_PATH = "./svbatcher/tests/test_sex.list"
COVERAGE_FILE_PATH = "./svbatcher/tests/test_coverage.tsv"
WGD_FILE_PATH = "./svbatcher/tests/test_wgd.tsv"
SEX_FILE_PATH = "./svbatcher/tests/test_sex.tsv"
OUT_DIR_PATH = "./svbatcher/tests/out"

TEST_TARGET_BATCH_SIZE = 200
TEST_COVERAGE_QUANTILES = 10
TEST_VERBOSITY = 1

TEST_MIN_ACCEPTABLE_BATCHES = 48
TEST_MAX_ACCEPTABLE_BATCHES = 52
TEST_MIN_ACCEPTABLE_BATCH_SIZE = 198
TEST_MAX_ACCEPTABLE_BATCH_SIZE = 202
