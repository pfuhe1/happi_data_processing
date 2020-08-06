#!/bin/bash 
#BSUB -q short-serial 
#BSUB -o %J.out 
#BSUB -e %J.err 
#BSUB -W 12:00

# Loop over MIPs
for MIP in CMIP5 CMIP6
do
	# Loop over indices and call scripts for this index and MIP
	for index in pryrmean RXx5day
	do
		for subset in subset regrid rcp26 rcp85
		do
			# Calculate regional means for IPCC AR5 regions from CMIP datasets. Split up results by model'
			# Process 'subset' of CMIP ensembles calculated above
			#python2.7 /home/users/pfu599/src/happi_data_processing/IPCC_regs_calc_indices_CMIPseparatemodels.py -o -i $index -d ${MIP}-${subset}
			# Calculate summary statistics from vales of indices for IPCC AR5 regions from CMIP datasets. Split up results by model
			# NOTE, the output summary dataset name is 'CMIP5-subset-weighted' and 'CMIP6-subset-weighted'
			#python2.7 /home/users/pfu599/src/happi_data_processing/IPCC_regs_summary_CMIPmodel_indices.py -o -i $index -d ${MIP}-${subset}

			python2.7 /home/users/pfu599/src/happi_data_processing/IPCC_regs_summary_CMIPmodel_indices_v4.py -o -i $index -d ${MIP}-${subset}

		done
	done
done
