#!/bin/bash

config_file="../../prism.ini"
config_name="default"

cli="python3 ../prism/cli.py --config_file $config_file --config_name=$config_name"

$cli buckets --help
echo "---------------------"

$cli buckets list --help
echo "---------------------"

$cli buckets create --help
echo "---------------------"

$cli buckets upload --help
echo "---------------------"

$cli buckets complete --help
echo "---------------------"


