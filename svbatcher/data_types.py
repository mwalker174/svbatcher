#!/usr/bin/env python

######################################################
#
# Sample and family data classes
# written by Mark Walker (markw@broadinstitute.org)
#
######################################################

from svbatcher.utils import printers


class Individual:
    _SEX_MALE = 1
    _SEX_FEMALE = 2
    _SEX_OTHER = 3
    TABLE_HEADER_STRING = "\t".join(["SAMPLE", "COVERAGE", "SEX", "WGD", "PROBAND"])
    MALE_CHAR = 'M'
    FEMALE_CHAR = 'F'
    SEX_OTHER_CHAR = 'U'

    def __init__(self, id):
        if not id:
            printers.raise_error("Tried to initialize Individual with ")
        self.id = str(id)
        self.coverage = None
        self.sex = None
        self.wgd = None
        self.proband = None

    def _proband_str(self):
        return "1" if self.is_proband() else "0"

    def __repr__(self):
        return "[" + self.id + "," + str(self.coverage) + "," + self.sex_char() + "," + str(self.wgd) + "," + self._proband_str() + "]"

    def table_string(self):
        return "\t".join([str(x) for x in [self.id, self.coverage, self.sex_char(), self.wgd, self._proband_str()]])

    def sex_char(self):
        return Individual.MALE_CHAR if self.is_male() else (Individual.FEMALE_CHAR if self.is_female() else Individual.SEX_OTHER_CHAR)

    def set_male(self):
        self.sex = Individual._SEX_MALE

    def set_female(self):
        self.sex = Individual._SEX_FEMALE

    def set_sex_other(self):
        self.sex = Individual._SEX_OTHER

    def is_male(self):
        return self.sex == Individual._SEX_MALE

    def is_female(self):
        return self.sex == Individual._SEX_FEMALE

    def is_sex_other(self):
        return self.sex == Individual._SEX_OTHER

    def is_proband(self):
        return self.proband

    def is_fully_defined(self):
        return self.id and self.coverage is not None and self.sex is not None and self.wgd is not None and self.proband is not None


class Family:
    TABLE_HEADER_STRING = "FAMILY" + "\t" + Individual.TABLE_HEADER_STRING

    def __init__(self, family_id, members):
        self.id = str(family_id)
        self.members = members
        probands = [x for x in members if x.is_proband()]
        if not probands:
            printers.print_warning("Family " + self.id + " contains no probands: " + str(self.members) + ", setting first member")
            self.proband = members[0]
        else:
            if len(probands) > 1:
                printers.print_warning("Family " + self.id + " contains multiple probands, using first")
            self.proband = probands[0]

    def size(self):
        return len(self.members)

    def num_male(self):
        return len([x for x in self.members if x.is_male()])

    def num_female(self):
        return len([x for x in self.members if x.is_female()])

    def num_sex_other(self):
        return len([x for x in self.members if x.is_sex_other()])

    def __repr__(self):
        return str(self.members)

    def table_strings(self):
        return [self.id + "\t" + x.table_string() for x in self.members]
