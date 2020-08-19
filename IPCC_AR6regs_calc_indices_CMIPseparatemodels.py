#!/usr/bin/env python
# Script to calculate the regional average of a variable/index
# region based on shapefile polygons
# Saves data to a pickle file
#
# Peter Uhe
# 27/02/2019

import numpy as np
import glob,os,socket,sys,argparse
import multiprocessing
import pickle
from netCDF4 import Dataset

home = os.environ.get('HOME')
argv = sys.argv

# Stuff to load masks
sys.path.append(os.path.join(home,'src/create_masks'))
from create_mask import load_shapefile_attrs

sys.path.append(os.path.join(home,'src/happi_data_processing'))
from basins_happi_alldata import get_basindata,create_mask



if __name__=='__main__':

	#######################################
	# Variables to set 

	parser = argparse.ArgumentParser('Script calculate regional means for IPCC AR5 regions from CMIP datasets. Split up results by model')
	parser.add_argument('-o','--override', default=False,action='store_true',help = 'Flag to override existing data or skip if an exeperiment has already been processed')
	parser.add_argument('-i','--index',default='RXx5day',help = 'index to process e.g. RXx5day,pryrmean')
	parser.add_argument('-d','--dataset',default = 'CMIP6-subset',help = 'CMIP datset to process [CMIP5-subset,CMIP6-subset]')
	parser.add_argument('-n','--num_threads',default = '16',type=int,help = 'Number of processes to use for parallel processing')
	args = parser.parse_args()
	print(args)

	# Set variables from arguments
	override = args.override
	index = args.index	
	dataset = args.dataset
	numthreads = args.num_threads

	# Set other varibles
	data_freq = 'N/A'
	var = 'pr' # Variable name in data files
	experiments = ['historical','slice15','slice20']

	
	#######################################
	# 	Paths/Variables  dependent on host/machine

	host=socket.gethostname()
	if host[:6] == 'jasmin' or host[-9:] == '.rl.ac.uk' or host[-12:]=='jasmin.ac.uk':
		shapefile = '/home/users/pfu599/data/shapefiles/reference_regions_AR6/reference_regions_split.shp'
		landmask_dir = '/home/users/pfu599/data/helix_landfrac'
		basepath = '/gws/nopw/j04/bris_climdyn/pfu599/timeslice_data/'
		data_pkl = '/home/users/pfu599/pkl/'+dataset+'models_'+index+'_AR6regs.pkl'
		mask_pkl = '/home/users/pfu599/pkl/AR6reg_masks.pkl'
		polygons_pkl = '/home/users/pfu599/pkl/AR6reg_polygons.pkl'
	else:
		raise Exception('ERROR, Unknown host: '+host)

	
	#######################################
	# load pickle files

	if os.path.exists(data_pkl):
		with open(data_pkl,'rb') as f_pkl:
			data_masked = pickle.load(f_pkl)
	else:
		data_masked = {}

	if os.path.exists(mask_pkl):
		with open(mask_pkl,'rb') as f_pkl:
			region_masks = pickle.load(f_pkl)
	else:
		region_masks = {}

	#######################################
	# load shapefile data

	print('loading region polygons...')
	reg_polygons,reg_attrs = load_shapefile_attrs(shapefile,'Acronym')
	with open(polygons_pkl,'wb') as f_pkl:
		pickle.dump(reg_polygons,f_pkl,-1)
		pickle.dump(reg_attrs,f_pkl,-1)
	#print('exiting...')
	#sys.exit(0)

	###########################################################################################

	# Load grid information
	f_landmask = glob.glob(landmask_dir+'/sftlf_fx_'+dataset+'_*.nc')[0]
	print(f_landmask)
	with Dataset(f_landmask,'r') as tmp:
		lat = tmp.variables['lat'][:]
		lon = tmp.variables['lon'][:]
		oceanpoints = tmp.variables['sftlf'][:]<50. # Land fractionless than 50%
		
	# Create 2D arrays of lon and lat
	nlat = len(lat)
	nlon = len(lon)
	lonxx,latyy=np.meshgrid(lon,lat)
	lonxx[lonxx>180]=lonxx[lonxx>180]-360
	points = np.vstack((lonxx.flatten(),latyy.flatten())).T
	
	##################################################################
	# Create masks
	print('Creating masks from polygons')
	pool = multiprocessing.Pool(processes=len(reg_polygons.keys()))
	if not dataset in region_masks:
		region_masks[dataset]={}
	results = {}
	for region,polygons in reg_polygons.items():
		if not region in region_masks[dataset]:
			results[region]=pool.apply_async(create_mask,(polygons,points,nlat,nlon))
	# close the pool and make sure the processing has finished
	pool.close()
	pool.join()
	print('collecting data...')
	for region,result in results.items():
			polymask = result.get(timeout=60.)
			use = reg_attrs[region]['Type']
			if region[-1]=='*':
				region = region[:-1] # remove * from name
			if use == 'Land': # Mask out ocean points
				if region in region_masks[dataset]: # Add to existing mask
					region_masks[dataset][region] = np.logical_and(region_masks[dataset][region],np.logical_or(polymask,oceanpoints))
				else:
					region_masks[dataset][region] = np.logical_or(polymask,oceanpoints)
			if use == 'Ocean': # Mask out land points
				if region in region_masks[dataset]: # Add to existing mask
					region_masks[dataset][region] = np.logical_and(region_masks[dataset][region],np.logical_or(polymask,np.logical_not(oceanpoints)))
				else:
					region_masks[dataset][region] = np.logical_or(polymask,np.logical_not(oceanpoints))
			elif use == 'Land-Ocean': # create separate masks for land a sea for this region
				region_masks[dataset][region+'_land']=np.logical_or(polymask,np.logical_not(oceanpoints))
				region_masks[dataset][region+'_sea']=np.logical_or(polymask,oceanpoints)
	# write out masks
	with open(mask_pkl,'wb') as f_pkl:
		pickle.dump(region_masks,f_pkl,-1)

	###########################################################################################
	# Loop over experiments and load data

	for experiment in experiments:
		print(experiment)
		file_lists = {}
		file_pattern = os.path.join(basepath,'indices_data/'+dataset+'/'+experiment+'/'+index+'/*_yrly.nc')
		print(file_pattern)
		# Get all paths for this dataset
		all_models = glob.glob(file_pattern)
		for fpath in all_models:
			fname = os.path.basename(fpath)
			if dataset[:4] == 'CMIP':
				model = fname.split('_')[5]
			elif dataset[:6] == 'UKCP18':
				ens = int(fname.split('_')[8])
				if ens<=15:
					model = 'HadGEM'
				else:
					model = str(ens) # just use ensemble number
			else:
				raise Exception('Error, script set up for CMIP or UKCP18, dataset is:',dataset)
			if not model in file_lists:
				file_lists[model]=[fpath]
			else:
				file_lists[model].append(fpath)

		##################################################################
		# Loop over models
		for model in file_lists.keys():
			# Load data
			if not model in data_masked:
				data_masked[model] = {}
			#file_pattern = 'indices_data/'+dataset+'/'+experiment+'/'+index+'/*_pr_day_'+model+'_*_yrly.nc'
			if not experiment in data_masked[model] or override:
				data_masked[model][experiment] = get_basindata(model,experiment,var,basepath,data_freq,numthreads=numthreads,masks=region_masks[dataset],file_pattern=file_lists[model])
				print(experiment,data_masked[model][experiment].keys())

	# write out data
	with open(data_pkl,'wb') as f_pkl:
		pickle.dump(data_masked,f_pkl,-1)

#	for model,expdata in data_masked.items():
#		print(model)
#		for expt,regdata in expdata.items():
#			print(expt,regdata['WSA'].shape)

