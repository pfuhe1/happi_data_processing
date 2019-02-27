#!/usr/bin/env python
# Script to load RXx5day data for the IPCC regions
#
# 1. Loads a pickle file containing region data
# Input data should be first calculated by running IPCC_regs_calc_RXx5day.py script
# 
# 2. Computes sampling uncertainty ranges and multi-model summary
#
# Peter Uhe
# 27/02/2019

import numpy as np
import pickle
import glob,os,socket,sys

home = os.environ.get('HOME')

# Import bootstrapping routine
sys.path.append(os.path.join(home,'src/happi_analysis/HAPPI_plots'))
from bootstrapping import bootstrap_mean_diff

if __name__=='__main__':
	
	#######################################
	# 	Paths/Variables  dependent on host/machine

	host=socket.gethostname()
	if host=='triassic.ggy.bris.ac.uk':
		basepath='/export/triassic/array-01/pu17449/happi_data_decade/'
		pkl_dir = '/export/silurian/array-01/pu17449/pkl_data/'
		models = ['CAM5-1-2-025degree']
		numthreads = 5
	elif host =='silurian.ggy.bris.ac.uk':
		basepath = '/export/silurian/array-01/pu17449/happi_data/'
		basin_path='/home/bridge/pu17449/src/happi_analysis/river_basins/basin_files/'
		pkl_dir = '/export/silurian/array-01/pu17449/pkl_data/'
		models = ['CESM-CAM5','NorESM1-HAPPI','MIROC5','CanAM4','CAM4-2degree','HadAM3P']
		markers = ['s','.','+','x','2','1']
		numthreads = 4
	elif host=='happi.ggy.bris.ac.uk':
		basepath = '/data/scratch/pu17449/happi_processed/'
		basin_path='/home/pu17449/happi_analysis/river_basins/basin_files/'
		pkl_dir = '/data/scratch/pu17449/pkl_data/'
		models = ['CESM-CAM5','NorESM1-HAPPI','MIROC5','CanAM4','CAM4-2degree','HadAM3P']
		markers = ['s','.','+','x','2','1']
		numthreads = 12
	elif host=='anthropocene.ggy.bris.ac.uk':
		data_pkl = '/export/anthropocene/array-01/pu17449/pkl/RXx5day_IPCCreg_data2.pkl'
		summary_pkl = '/export/anthropocene/array-01/pu17449/pkl/RXx5day_IPCCreg_summary2.pkl'
		models = ['NorESM1-HAPPI','MIROC5','CanAM4','CAM4-2degree','HadAM3P','ECHAM6-3-LR','CAM5-1-2-025degree']
		summary_name = 'HAPPI'
		numthreads = 12
	elif host[:6] == 'jasmin' or host[-11:] == 'jc.rl.ac.uk':
		data_pkl = '/home/users/pfu599/pkl/RXx5day_IPCCregs.pkl'
		summary_pkl = '/home/users/pfu599/pkl/RXx5day_IPCCregs_MMsummary.pkl'
		numthreads = 8
		models = ['ec-earth3-hr','hadgem3']#,'EC-EARTH3-HR','HadGEM3']
		summary_name = 'helix_biascorr'
		
	#######################################
	# Variables to set
	
	data_freq = 'N/A'
	var = 'pr'
	index = 'RXx5day'

	scenarios = ['1.5$^{\circ}$C - Hist','2$^{\circ}$C - Hist','2$^{\circ}$C - 1.5$^{\circ}$C']
	
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

	###########################################################################
	# Initialise arrays for summary statistics
	pct_ch_arr = {}
	pct_ch_up = {}
	pct_ch_down = {}
	for reg in region_masks[model].keys():
		pct_ch_arr[reg] = np.zeros([len(models),len(scenarios)])
		pct_ch_up[reg] = np.zeros([len(models),len(scenarios)])
		pct_ch_down[reg] = np.zeros([len(models),len(scenarios)])

	#########################################################################
	# Load data

	for z,model in enumerate(models):

		# Set experiments
		if model =='CESM-CAM5':
			experiments = ['historical','1pt5degC','2pt0degC']
			scale = 1000.
		elif model == 'CMIP5':
			experiments = ['historical','slice15','slice20']
			scale = 1.
		else:
			experiments = ['All-Hist','Plus15-Future','Plus20-Future']
			scale = 1.

		# Save sampling uncertainty data for each region
		print('Calculating uncertainty ranges')
		for reg in regs:
			seas_data = []
			# Flatten data for this model/region into 'seas_data' array
			for experiment in experiments:
				seas_data.append((data_masked[model][experiment][reg]*scale).compressed())
		
			for d,scen in enumerate(scenarios):
				# calculate bootstrapped error for mean:
				print 'datahape',seas_data[0].shape,seas_data[1].shape
				if d!=2: # 2deg and 1.5deg vs Hist
					pct_change = bootstrap_mean_diff(seas_data[d+1],seas_data[0])
				else: # 2deg vs 1.5deg 
					pct_change = bootstrap_mean_diff(seas_data[d],seas_data[d-1]) 
				pct_ch_arr[reg][z,d]=pct_change[1]
				pct_ch_up[reg][z,d]=pct_change[2]
				pct_ch_down[reg][z,d]=pct_change[0]
				#print model,scen,'mean',pct_change

	############################################################################
	# Now create meta analysis / multi model summary:

	print('Calculating multi model summary data')
	for reg in regs:
		# Initialise summary dictionary
		if not summary.has_key(reg):
			print(reg)
			summary[reg]={}
		# If the summary has not already been created for this ensemble
		if not summary[reg].has_key(summary_name):
			summary[reg][summary_name]={}
			for d,scen in enumerate(scenarios):
				# Use random effect meta analysis 
				model_spr = pct_ch_arr[reg][:,d].std()**2
				sample_var = ((pct_ch_up[reg][:,d]-pct_ch_down[reg][:,d])/3.2)**2 # Assume normal distribution, 5-95% range is 3.2 times std
				model_w = 1./(model_spr + sample_var[z])
		
				best_est = (model_w*pct_ch_arr[reg][:,d]).sum()/model_w.sum()
				best_err = 1.6*(1/ model_w.sum())**0.5

				summary[reg][summary_name][scen]=[best_est-best_err,best_est,best_est+best_err]
				
	############################################################################			
	# write out data
	with open(summary_pkl,'wb') as f_pkl:
		pickle.dump(summary,f_pkl,-1)

