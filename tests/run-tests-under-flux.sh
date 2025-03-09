#!/bin/bash
set -e
flux resource info
echo $FLUX_URI

nvtest run -w -b scheduler=flux -b scheme=isolate .
