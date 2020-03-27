#!/usr/bin/env python

######################################################
#
# Disk read/write utils
# written by Mark Walker (markw@broadinstitute.org)
#
######################################################

from svbatcher.data_types import Individual, Family
from svbatcher.utils import printers
from os import path
import gzip

# Properties of expected input data
HEADER_SYMBOL = '#'
COLUMN_DELIM = '\t'

COHORT_SAMPLE_ID_COLUMN_NAME = "ID"
COHORT_COVERAGE_COLUMN_NAME = "coverage"

PED_FAMILY_ID_COLUMN = 0
PED_SAMPLE_ID_COLUMN = 1
PED_PROBAND_COLUMN = 5
PED_PROBAND_VALUE = "2"

SEX_ASSIGNMENT_SAMPLE_COLUMN_NAME = "ID"
SEX_ASSIGNMENT_SEX_COLUMN_NAME = "Assignment"
SEX_ASSIGNMENT_MALE_STRING = "MALE"
SEX_ASSIGNMENT_FEMALE_STRING = "FEMALE"

WGD_SAMPLE_COLUMN_NAME = "ID"
WGD_SCORE_COLUMN_NAME = "score"


def open_possibly_gzipped(file_path, mode):
    return gzip.open(file_path, mode) if file_path.endswith('.gz') else open(file_path, mode)


def _get_column_index(header, name):
    header_tokens = header.strip().split(COLUMN_DELIM)
    if not header_tokens:
        printers.raise_error("Empty header fields")
    if not header_tokens[0].startswith(HEADER_SYMBOL):
        printers.raise_error("Expected header to start with '#' but the header was: \"" + "\t".join(header_tokens) + "\"")
    header_tokens[0] = header_tokens[0][1:]
    return header_tokens.index(name)


# If individuals_dict is not provided or is None, the data is populated from sample id's in the file.
# If individuals_dict is provided, no new Individuals will be added
def _read_data(filename, sample_id_column_name, metric_column_name, metric_assignment_func, individuals_dict=None):
    if individuals_dict is None:
        individuals_dict = {}
        add_new_samples = True
    else:
        add_new_samples = False

    with open_possibly_gzipped(filename, 'r') as f:
        header = f.readline()
        sample_id_index = _get_column_index(header, sample_id_column_name)
        metric_index = _get_column_index(header, metric_column_name)
        header_tokens = header.strip().split(COLUMN_DELIM)
        num_columns = len(header_tokens)
        for line in f:
            if line.startswith(HEADER_SYMBOL):
                printers.raise_error("Expected only 1 header line starting with '" + HEADER_SYMBOL + "'")
            tokens = line.strip().split('\t')
            if len(tokens) != num_columns:
                printers.raise_error("There are " + str(num_columns) + " columns in the header, but only " + str(len(tokens)) + " columns in the line: \"" + line.strip() + "\"")
            sample_id = tokens[sample_id_index]
            metric = tokens[metric_index]
            if add_new_samples and not sample_id in individuals_dict:
                individuals_dict[sample_id] = Individual(sample_id)
            if sample_id in individuals_dict:
                metric_assignment_func(individuals_dict[sample_id], metric)
    return individuals_dict


def _assign_coverage(individual, coverage):
    individual.coverage = float(coverage)


def _assign_sex(individual, sex):
    if sex == SEX_ASSIGNMENT_MALE_STRING:
        individual.set_male()
    elif sex == SEX_ASSIGNMENT_FEMALE_STRING:
        individual.set_female()
    else:
        individual.set_sex_other()


def _assign_dosage_score(individual, wgd):
    individual.wgd = float(wgd)


def read_coverage_file(filename, individuals_dict=None):
    return _read_data(filename, COHORT_SAMPLE_ID_COLUMN_NAME, COHORT_COVERAGE_COLUMN_NAME, _assign_coverage, individuals_dict=individuals_dict)


def read_sex_assignment_file(filename, individuals_dict=None):
    return _read_data(filename, SEX_ASSIGNMENT_SAMPLE_COLUMN_NAME, SEX_ASSIGNMENT_SEX_COLUMN_NAME, _assign_sex, individuals_dict=individuals_dict)


def read_sex_assignment_list(file_list_path, individuals_dict):
    with open_possibly_gzipped(file_list_path, 'r') as f:
        for line in f:
            individuals_dict = read_sex_assignment_file(line.strip(), individuals_dict=individuals_dict)
    return individuals_dict


def read_wgd_file(filename, individuals_dict=None):
    return _read_data(filename, WGD_SAMPLE_COLUMN_NAME, WGD_SCORE_COLUMN_NAME, _assign_dosage_score, individuals_dict=individuals_dict)


def read_wgd_list(file_list_path, individuals_dict):
    with open_possibly_gzipped(file_list_path, 'r') as f:
        for line in f:
            individuals_dict = read_wgd_file(line.strip(), individuals_dict=individuals_dict)
    return individuals_dict


# Reads ped file and creates families from existing population of Individuals
def assign_families(ped_file, individuals_dict):
    families = {}
    with open(ped_file, 'r') as f:
        for line in f:
            if line.startswith(HEADER_SYMBOL):
                continue
            tokens = line.strip().split('\t')
            num_cols = len(tokens)
            if num_cols < PED_SAMPLE_ID_COLUMN+1 or num_cols < PED_FAMILY_ID_COLUMN+1:
                printers.raise_error("Not enough columns in PED file line: \"" + line.strip() + "\"")
            family_id = tokens[PED_FAMILY_ID_COLUMN]
            sample_id = tokens[PED_SAMPLE_ID_COLUMN]
            if sample_id in individuals_dict:
                if tokens[PED_PROBAND_COLUMN] == PED_PROBAND_VALUE:
                    individuals_dict[sample_id].proband = True
                else:
                    individuals_dict[sample_id].proband = False
                if family_id not in families:
                    families[family_id] = []
                families[family_id].append(individuals_dict[sample_id])
    return [Family(family_id, families[family_id]) for family_id in families]


def write_output(batches, output_dir):
    for i in range(len(batches)):
        file_path = path.join(output_dir, "batch." + str(i) + ".txt")
        with open(file_path, 'w') as f:
            f.write("#" + Family.TABLE_HEADER_STRING + "\n")
            batch_out = batches[i]
            for family in batch_out:
                f.write("\n".join(family.table_strings()) + "\n")
