#!/usr/bin/env python
# Script to load climate indices data for the IPCC regions
#
# 1. Loads a pickle file containing region data
# Input data should be first calculated by running IPCC_regs_calc_index.py script
# 
# 2. Computes sampling uncertainty ranges and multi-model summary
#
# Peter Uhe
# 27/02/2019

import numpy as np
import pickle
import glob,os,socket,sys,argparse

home = os.environ.get('HOME')
argv = sys.argv

# Import bootstrapping routine
sys.path.append(os.path.join(home,'src/happi_analysis/HAPPI_plots'))
from bootstrapping import bootstrap_mean_diff

if __name__=='__main__':

	#######################################
	# Variables to set

	parser = argparse.ArgumentParser('Script to calculate summary statistics from vales of indices for IPCC AR5 regions from CMIP datasets. Split up results by model')
	parser.add_argument('-o','--override', default=False,action='store_true',help = 'Flag to override existing data or skip if an exeperiment has already been processed')
	parser.add_argument('-i','--index',default='RXx5day',help = 'index to process e.g. RXx5day,pryrmean')
	parser.add_argument('-d','--dataset',default = 'CMIP6-subset',help = 'CMIP datset to process [CMIP5-subset,CMIP6-subset]')
	parser.add_argument('-n','--num_threads',default = '8',type=int,help = 'Number of processes to use for parallel processing')
	args = parser.parse_args()
	print(args)

	# Set variables from arguments
	override = args.override
	index = args.index	
	dataset = args.dataset
	numthreads = args.num_threads

	# Set other variables
	scenarios = ['1.5$^{\circ}$C - Hist','2$^{\circ}$C - Hist','2$^{\circ}$C - 1.5$^{\circ}$C']

	
	#######################################
	# 	Paths/Variables  dependent on host/machine

	host=socket.gethostname()
	if host[:6] == 'jasmin' or host[-11:] == 'jc.rl.ac.uk':
		data_pkl = '/home/users/pfu599/pkl/'+dataset+'models_'+index+'_IPCCregs.pkl'
		cmip_summary_pkl = '/home/users/pfu599/pkl/'+dataset+'models_'+index+'_IPCCregs_jasmin_summary.pkl'
		summary_pkl = '/home/users/pfu599/pkl/'+index+'_IPCCregs_jasmin_summary.pkl'
		summary_name = dataset+'-weighted'
	else:
		raise Exception('Error, this script has only been set up to run on JASMIN')
		
	#######################################
	# load pickle files

	if os.path.exists(summary_pkl):
		with open(summary_pkl,'rb') as f_pkl:
			summary = pickle.load(f_pkl)
	else:
		summary = {}

	if os.path.exists(cmip_summary_pkl):
		with open(cmip_summary_pkl,'rb') as f_pkl:
			cmip_summary = pickle.load(f_pkl)
	else:
		cmip_summary = {}


	if os.path.exists(data_pkl):
		with open(data_pkl,'rb') as f_pkl:
			data_masked = pickle.load(f_pkl)
	else:
		raise Exception('First calculate region data and save to pkl: '+data_pkl)

	#######################################
	# Get regions
	regs = data_masked.values()[0].values()[0].keys()
	print('regions',regs)

	# quick quality control:
	for model,modeldata in data_masked.items():
		if len(modeldata)<3:
			del(data_masked[model])

	# Get models
	models = data_masked.keys()


	###########################################################################
	# Initialise arrays for summary statistics
	pct_ch_arr = {}
	pct_ch_up = {}
	pct_ch_down = {}
	for reg in regs:
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
		elif model == 'CMIP5' or host[:6] == 'jasmin' or host[-11:] == 'jc.rl.ac.uk':
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
				# Initialise cmip_summary dictionary then set values
				if not reg in cmip_summary:
					cmip_summary[reg]={}
				if not model in cmip_summary[reg]:
					cmip_summary[reg][model]={}
				cmip_summary[reg][model][scen] = [pct_change[0],pct_change[1],pct_change[2]]

	############################################################################
	# Now create meta analysis / multi model summary:

	print('Calculating multi model summary data')
	for reg in regs:
		# Initialise summary dictionary
		if not summary.has_key(reg):
			print(reg)
			summary[reg]={}
		# If the summary has not already been created for this ensemble
		if not summary[reg].has_key(summary_name) or override:
			summary[reg][summary_name]={}
			for d,scen in enumerate(scenarios):
				# Use random effect meta analysis 
				model_spr = pct_ch_arr[reg][:,d].std()**2
				sample_var = ((pct_ch_up[reg][:,d]-pct_ch_down[reg][:,d])/3.2)**2 # Assume normal distribution, 5-95% range is 3.2 times std
				model_w = 1./(model_spr + sample_var[:])
		
				best_est = (model_w*pct_ch_arr[reg][:,d]).sum()/model_w.sum()
				best_err = 1.6*(1/ model_w.sum())**0.5

				summary[reg][summary_name][scen]=[best_est-best_err,best_est,best_est+best_err]
				if reg == 'SAS_land' and d==2:
					print('Debug, SAS_land, 1p5vs2')
					print('debug: model best',pct_ch_arr[reg][:,d])
					print('debug: mode weights',model_w)
					print('debug: best',best_est,best_err)
				
	############################################################################			
	# write out data
	with open(summary_pkl,'wb') as f_pkl:
		pickle.dump(summary,f_pkl,-1)

	# write out data
	with open(cmip_summary_pkl,'wb') as f_pkl:
		pickle.dump(cmip_summary,f_pkl,-1)

