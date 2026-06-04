#!/bin/sh
set -eu

echo "Expected declared limits:"
echo "  lb   cpu=0.90  memory=320MB"
echo "  api1 cpu=0.05  memory=12MB"
echo "  api2 cpu=0.05  memory=12MB"
echo "  total cpu=1.0 memory=344MB"
