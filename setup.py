#!/usr/bin/env python

from setuptools import setup

setup(name='svbatcher',
      version='0.1',
      description='Cohort batching package for the GATK SV pipeline',
      classifiers=[
          'Development Status :: 3 - Alpha',
          'Programming Language :: Python :: 2.7',
      ],
      url='https://github.com/talkowski-lab/gatk-sv-v1',
      author='Mark Walker',
      author_email='markw@broadinsitute.org',
      packages=['svbatcher'],
      include_package_data=True,
      zip_safe=False,
      entry_points = {
            'console_scripts': ['sv-batcher=svbatcher.command_line:main'],
      },
      test_suite='nose.collector',
      tests_require=['nose'])
