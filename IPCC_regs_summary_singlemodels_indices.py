#!/usr/bin/env python
# Script to load climate indices data for the IPCC regions
#
# 1. Loads a pickle file containing region data
# Input data should be first calculated by running IPCC_regs_calc_indices.py script
#
# 2. Computes sampling uncertainty ranges for each model/region and saves to a new pkl file
#
# Peter Uhe
# 27/02/2019

import numpy as np
import pickle
import glob,os,socket,sys

home = os.environ.get('HOME')
argv = sys.argv

# Import bootstrapping routine
sys.path.append(os.path.join(home,'src/happi_analysis/HAPPI_plots'))
from bootstrapping import bootstrap_mean_diff

if __name__=='__main__':

	#######################################
	# Variables to set

	override = True
	data_freq = 'N/A'
	var = 'pr'
	if len(argv)>1:
		index = argv[1].strip()
	else:
		index = 'RXx5day'
	scenarios = ['1.5$^{\circ}$C - Hist','2$^{\circ}$C - Hist','2$^{\circ}$C - 1.5$^{\circ}$C']

	
	#######################################
	# 	Paths/Variables  dependent on host/machine

	host=socket.gethostname()
	if host=='anthropocene.ggy.bris.ac.uk':
		data_pkl = '/export/anthropocene/array-01/pu17449/pkl/'+index+'_IPCCreg_data3.pkl'
		summary_pkl = '/export/anthropocene/array-01/pu17449/pkl/'+index+'_IPCCreg_summary3.pkl'
		#models = ['CMIP5-1permodel','CESM-CAM5']
		#models = ['NorESM1-HAPPI','MIROC5','CanAM4','CAM4-2degree','HadAM3P','ECHAM6-3-LR','CAM5-1-2-025degree','CESM-CAM5-LW','CESM-CAM5-LE']
		models = ['CanESM2']
		numthreads = 12
	elif host[:6] == 'jasmin' or host[-11:] == 'jc.rl.ac.uk':
		data_pkl = '/home/users/pfu599/pkl/'+index+'_IPCCregs.pkl'
		summary_pkl = '/home/users/pfu599/pkl/'+index+'_IPCCregs_jasmin_summary.pkl'
		numthreads = 8
		#models = ['ec-earth3-hr','hadgem3','EC-EARTH3-HR','HadGEM3','CMIP6-regrid','CMIP6-1permodel','CMIP6-subset','UKCP18-global']
		#models = ['CMIP5-subset','CMIP5-regrid','CMIP5-1permodel']
		models = ['CMIP5-subset','CMIP6-subset','UKCP18-hadgem']

	#######################################
	# load pickle files

	if os.path.exists(summary_pkl):
		with open(summary_pkl,'rb') as f_pkl:
			summary = pickle.load(f_pkl)
	else:
		summary = {}

	if os.path.exists(data_pkl):
		with open(data_pkl,'rb') as f_pkl:
			data_masked = pickle.load(f_pkl)
	else:
		raise Exception('First calculate region data and save to pkl: '+data_pkl)

	#######################################
	# Get regions
	regs = data_masked.values()[0].values()[0].keys()
	print('regions',regs)

	#########################################################################
	# Load data

	for z,model in enumerate(models):
		print('\nLoading Model:',model)

		# Set experiments
		if model =='CESM-CAM5' or model=='CESM-CAM5-LW':
			experiments = ['historical','1pt5degC','2pt0degC']
			scale = 1000.
		# CMIP5 or Helix models
		elif model[:4] == 'CMIP' or model == 'CanESM2' or host[:6] == 'jasmin' or host[-11:] == 'jc.rl.ac.uk' or model == 'CESM-CAM5-LE': 
			experiments = ['historical','slice15','slice20']
			scale = 1.
		else:
			experiments = ['All-Hist','Plus15-Future','Plus20-Future']
			scale = 1.

		# Save sampling uncertainty data for each region
		print('Calculating uncertainty ranges')
		for reg in regs:
			print(reg)
			# Initialise summary dictionary
			if not summary.has_key(reg):
				summary[reg]={}
			# Create summary for this model if doesnt exist already
			if not summary[reg].has_key(model) or override:
				summary[reg][model]={}

				# Flatten data for this model/region into 'seas_data' array
				seas_data = []
				for experiment in experiments:
					seas_data.append((data_masked[model][experiment][reg]*scale).compressed())

				# Loop over scenarios and calculate uncertainty ranges		
				for d,scen in enumerate(scenarios):
					# calculate bootstrapped error for mean:
					print 'datahape',seas_data[0].shape,seas_data[1].shape
					if d!=2: # 2deg and 1.5deg vs Hist
						pct_change = bootstrap_mean_diff(seas_data[d+1],seas_data[0])
					else: # 2deg vs 1.5deg 
						pct_change = bootstrap_mean_diff(seas_data[d],seas_data[d-1]) 
					#print model,scen,'mean',pct_change
					summary[reg][model][scen]=[pct_change[0],pct_change[1],pct_change[2]]
				
	#########################################################################				
	# write out data
	with open(summary_pkl,'wb') as f_pkl:
		pickle.dump(summary,f_pkl,-1)

