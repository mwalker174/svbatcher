#!/usr/bin/env python

######################################################
#
# Std io utils
# written by Mark Walker (markw@broadinstitute.org)
#
######################################################

import sys


def raise_error(msg):
    raise ValueError(msg + "\n")


def print_warning(msg):
    sys.stderr.write("Warning: " + msg + "\n")


def print_parameter(name, val):
    if isinstance(val, list):
        sys.stderr.write(name + ": " + " / ".join([str(v) for v in val]) + "\n")
    else:
        sys.stderr.write(name + ": " + str(val) + "\n")