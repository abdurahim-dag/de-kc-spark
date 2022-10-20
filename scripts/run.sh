#!/bin/bash

set -e

JOB="main.py"

poetry run spark-submit \
    --master local \
    "${JOB}"