#!/usr/bin/env python

import svbatcher.tests.constants as const
import svbatcher.utils.io as io
import random
import os

SEED = 0
COHORT_SIZE = const.TEST_COHORT_SIZE

WGD_FILE_LIST_PATH = const.WGD_FILE_LIST_PATH
SEX_FILE_LIST_PATH = const.SEX_FILE_LIST_PATH
COVERAGE_FILE_PATH = const.COVERAGE_FILE_PATH
WGD_FILE_PATH = const.WGD_FILE_PATH
SEX_FILE_PATH = const.SEX_FILE_PATH

COVERAGE_MIN = 20.0
COVERAGE_MAX = 50.0
WGD_MIN = -1.0
WGD_MAX = 1.0
SEX_MALE_STR = io.SEX_ASSIGNMENT_MALE_STRING
SEX_FEMALE_STR = io.SEX_ASSIGNMENT_FEMALE_STRING
SEX_OTHER_STR = "OTHER"

PROB_MALE = 0.49
PROB_FEMALE = 0.49
PROB_OTHER = 1.0 - PROB_MALE - PROB_FEMALE

NUM_COVERAGE_FIELDS = 2
COVERAGE_VALUE_FIELD = 1
NUM_WGD_FIELDS = 2
WGD_VALUE_FIELD = 1
NUM_SEX_FIELDS = 8
SEX_VALUE_FIELD = 3

COVERAGE_HEADER = "\t".join([io.HEADER_SYMBOL + io.COHORT_SAMPLE_ID_COLUMN_NAME, io.COHORT_COVERAGE_COLUMN_NAME]) + "\n"
WGD_HEADER = "\t".join([io.HEADER_SYMBOL + io.WGD_SAMPLE_COLUMN_NAME, io.WGD_SCORE_COLUMN_NAME]) + "\n"
SEX_HEADER = "\t".join([io.HEADER_SYMBOL + io.SEX_ASSIGNMENT_SAMPLE_COLUMN_NAME, "chrX.CN", "chrY.CN", io.SEX_ASSIGNMENT_SEX_COLUMN_NAME, "pMos.X", "pMos.Y", "qMos.X", "qMos.Y"]) + "\n"
EMPTY_FIELD_STR = "na"


def get_random_sex():
    r = random.random()
    if r < PROB_MALE:
        return SEX_MALE_STR
    if r < PROB_FEMALE + PROB_MALE:
        return SEX_FEMALE_STR
    return SEX_OTHER_STR


def generate_data():
    random.seed(SEED)
    sample_ids = []
    coverage = []
    wgd = []
    sex = []
    for i in range(COHORT_SIZE):
        sample_ids.append("sample_" + str(i))
        coverage.append(random.uniform(COVERAGE_MIN, COVERAGE_MAX))
        wgd.append(random.uniform(WGD_MIN, WGD_MAX))
        sex.append(get_random_sex())
    return sample_ids, coverage, wgd, sex


def write_list(file_path, list):
    with open(file_path, 'w') as f:
        for item in list:
            f.write(item + "\n")


def write_tsv(file_path, sample_ids, values, num_fields, value_field, header):
    with open(file_path, 'w') as f:
        f.write(header)
        for i in range(len(sample_ids)):
            fields = []
            for j in range(num_fields):
                if j == 0:
                    fields.append(sample_ids[i])
                elif j == value_field:
                    fields.append(str(values[i]))
                else:
                    fields.append(EMPTY_FIELD_STR)
            f.write("\t".join(fields) + "\n")


def generate_and_write_files():
    sample_ids, coverage, wgd, sex = generate_data()
    write_list(WGD_FILE_LIST_PATH, [WGD_FILE_PATH])
    write_list(SEX_FILE_LIST_PATH, [SEX_FILE_PATH])
    write_tsv(COVERAGE_FILE_PATH, sample_ids, coverage, NUM_COVERAGE_FIELDS, COVERAGE_VALUE_FIELD, COVERAGE_HEADER)
    write_tsv(WGD_FILE_PATH, sample_ids, wgd, NUM_WGD_FIELDS, WGD_VALUE_FIELD, WGD_HEADER)
    write_tsv(SEX_FILE_PATH, sample_ids, sex, NUM_SEX_FIELDS, SEX_VALUE_FIELD, SEX_HEADER)


def delete_files():
    os.remove(WGD_FILE_LIST_PATH)
    os.remove(SEX_FILE_LIST_PATH)
    os.remove(COVERAGE_FILE_PATH)
    os.remove(WGD_FILE_PATH)
    os.remove(SEX_FILE_PATH)
