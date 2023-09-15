#!/bin/bash

config_file="../../prism.ini"
config_name="default"

cli="python3 ../prism/cli.py --config_file $config_file --config_name=$config_name"

$cli tables --help
echo "---------------------"

$cli tables list --help
echo "---------------------"

$cli tables create --help
echo "---------------------"

$cli tables upload --help
echo "---------------------"

$cli tables update --help
echo "---------------------"

# Bad table name
$cli tables update bob 
