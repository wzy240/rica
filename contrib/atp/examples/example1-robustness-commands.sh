#!/usr/bin/env bash
set -euo pipefail

# Example 1 parameter-robustness command generator for atp-single-layer.
# It only prints commands; pipe to a shell later if you want to execute them.

waiting_thresholds=(0 10 20 40 80)
sample_thresholds=(5 20 80)
background_modes=(false true)

for has_bg in "${background_modes[@]}"; do
  for waiting_us in "${waiting_thresholds[@]}"; do
    for sample_threshold in "${sample_thresholds[@]}"; do
      printf './ns3 run "atp-single-layer --hasJob1BackgroundFlow=%s --stragglerWaitingThresholdUs=%s --stragglerSampleThreshold=%s"\n' \
        "${has_bg}" "${waiting_us}" "${sample_threshold}"
    done
  done
done
