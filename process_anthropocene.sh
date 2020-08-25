#!/usr/bin/env bash
# Processing for multi MIP comparison: datasets on anthropocene:

# Relies on first calculating timeslices for CESM-CAM5 and CanESM2

# calculate indices for RXx5day, RXx1day and pryrmean
#python indices_portaldata_clim_v4.py # default processes HAPPI models
#python indices_portaldata_clim_v4.py CESM-CAM5
#python indices_portaldata_clim_v4.py CanESM2

# Loop over indices and call scripts for this index and MIP
for index in RXx5day RXx1day pryrmean
do
  # Calculate region averages for AR6 regions (script loops over models)
  python IPCC_AR6regs_calc_indices.py $index

  # Calculate summary statistics from vales of indices for IPCC AR6 regions
  python IPCC_AR6regs_summary_multimodel_indices.py $index #(default is HAPPI models only)
  python IPCC_AR6regs_absprecipsummary_multimodel_indices.py $index #(default is HAPPI models only)

done
