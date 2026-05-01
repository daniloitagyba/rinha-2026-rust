#!/bin/sh
set -eu

echo "Expected declared limits:"
echo "  lb   cpu=0.20  memory=32MB"
echo "  api1 cpu=0.40  memory=156MB"
echo "  api2 cpu=0.40  memory=156MB"
echo "  total cpu=1.0 memory=344MB"
