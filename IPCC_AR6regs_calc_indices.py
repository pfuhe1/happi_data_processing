#!/usr/bin/env python
# Script to calculate the regional average of a variable/index
# region based on shapefile polygons
# Saves data to a pickle file
#
# Peter Uhe
# 27/02/2019

import numpy as np
import glob,os,socket,sys
import multiprocessing
import pickle
from netCDF4 import Dataset
import matplotlib.pyplot as plt

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

	data_freq = 'N/A'
	override = True
	var = 'pr'
	if len(argv)>1:
		index = argv[1].strip()
	else:
		index = 'RXx5day'

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
		models = ['CanESM2',]#'CESM-CAM5','NorESM1-HAPPI','MIROC5','CanAM4','CAM4-2degree','HadAM3P','ECHAM6-3-LR','CAM5-1-2-025degree']
		markers = ['s','.','+','x','2','1']
		numthreads = 12
	elif host=='anthropocene.ggy.bris.ac.uk':
		shapefile = '/export/anthropocene/array-01/pu17449/shapefiles/reference_regions_AR6/reference_regions_split.shp'
		landmask_dir = '/export/anthropocene/array-01/pu17449/happi_data/landmask'
		basepath = '/export/anthropocene/array-01/pu17449/happi_processed/'
		data_pkl = '/export/anthropocene/array-01/pu17449/pkl/AR6regs/'+index+'_AR6reg_data3.pkl'
		mask_pkl = '/export/anthropocene/array-01/pu17449/pkl/AR6regs/AR6reg_masks2.pkl'
		summary_pkl = '/export/anthropocene/array-01/pu17449/pkl/AR6regs/'+index+'_AR6reg_summary3.pkl'
		polygons_pkl = '/export/anthropocene/array-01/pu17449/pkl/AR6regs/AR6reg_polygons_closed.pkl'
		models = ['CanESM2']#'NorESM1-HAPPI','MIROC5','CanAM4','CAM4-2degree','HadAM3P','ECHAM6-3-LR','CAM5-1-2-025degree','CESM-CAM5-LW','CESM-CAM5-LE','CanESM2']
		#models = ['CESM-CAM5-LE',IPCC_AR6regs_calc_indices.py]
		numthreads = 4
	elif host[:6] == 'jasmin' or host[-11:] == 'jc.rl.ac.uk' or host[-12:]=='jasmin.ac.uk':
		shapefile = '/home/users/pfu599/data/shapefiles/reference_regions_AR6/reference_regions_split.shp'
		landmask_dir = '/home/users/pfu599/data/helix_landfrac'
		#basepath = '/work/scratch/pfu599/helix_data/processed_data/'
		#basepath = '/work/scratch-nompiio/pfu599/timeslice_data/'
		basepath = '/gws/nopw/j04/bris_climdyn/pfu599/timeslice_data/'
		data_pkl = '/home/users/pfu599/pkl/'+index+'_AR6regs.pkl'
		mask_pkl = '/home/users/pfu599/pkl/AR6reg_masks.pkl'
		polygons_pkl = '/home/users/pfu599/pkl/AR6reg_polygons_closed.pkl'
		numthreads = 8
		models = ['ec-earth3-hr','hadgem3','EC-EARTH3-HR','HadGEM3']#,'CMIP6-regrid','CMIP6-1permodel','CMIP6-subset','UKCP18-global']
		#models = ['CMIP5','CMIP6','UKCP18-global']
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
	# Loop over models
	for model in models:
		print(model)

		# Set experiments
		if model =='CESM-CAM5' or model=='CESM-CAM5-LW':
			experiments = ['historical','1pt5degC','2pt0degC']#,'slice15','slice20']
		elif model[:4] == 'CMIP' or model == 'CanESM2' or host[:6] == 'jasmin' or host[-11:] == 'jc.rl.ac.uk' or model=='CESM-CAM5-LE' or host[-12:]=='jasmin.ac.uk':
			experiments = ['historical','slice15','slice20']
		else:
			experiments = ['All-Hist','Plus15-Future','Plus20-Future']

		# Load grid information
		f_landmask = glob.glob(landmask_dir+'/sftlf_fx_'+model+'_*.nc')[0]
		print(f_landmask)
		with Dataset(f_landmask,'r') as tmp:
			lat = tmp.variables['lat'][:]
			lon = tmp.variables['lon'][:]
			oceanpoints = tmp.variables['sftlf'][:]<50. # Land fractionless than 50%

		# Create 2D arrays of lon and lat
		nlat = len(lat)
		nlon = len(lon)
		lonxx,latyy=np.meshgrid(lon,lat)
		# Convert lon range from 0-360 to -180-180
		lonxx[lonxx>180]=lonxx[lonxx>180]-360
		points = np.vstack((lonxx.flatten(),latyy.flatten())).T

		##################################################################
		# Create masks
		print('Creating masks from polygons')
		pool = multiprocessing.Pool(processes=len(reg_polygons.keys()))
		if not model in region_masks:
			region_masks[model]={}
		results = {}
		for region,polygons in reg_polygons.items():
			if not region in region_masks[model]:
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
				if region in region_masks[model]: # Add to existing mask
					region_masks[model][region] = np.logical_and(region_masks[model][region],np.logical_or(polymask,oceanpoints))
				else:
					region_masks[model][region] = np.logical_or(polymask,oceanpoints)
			if use == 'Ocean': # Mask out land points
				if region in region_masks[model]: # Add to existing mask
					region_masks[model][region] = np.logical_and(region_masks[model][region],np.logical_or(polymask,np.logical_not(oceanpoints)))
				else:
					region_masks[model][region] = np.logical_or(polymask,np.logical_not(oceanpoints))
			elif use == 'Land-Ocean': # create separate masks for land a sea for this region
				region_masks[model][region+'_land']=np.logical_or(polymask,np.logical_not(oceanpoints))
				region_masks[model][region+'_sea']=np.logical_or(polymask,oceanpoints)

			# debug plot mask
			#if use=='Ocean':
			#	plt.figure()
			#	plt.pcolormesh(region_masks[model][region])
			#	plt.title(region)
			#	plt.show()

		# write out masks
		with open(mask_pkl,'wb') as f_pkl:
			pickle.dump(region_masks,f_pkl,-1)

		##################################################################
		# Load data
		if not model in data_masked:
			data_masked[model] = {}
		for experiment in experiments:
			file_pattern = 'indices_data/'+model+'/'+experiment+'/'+index+'/*_yrly.nc'
			if not experiment in data_masked[model] or override:
				data_masked[model][experiment] = get_basindata(model,experiment,var,basepath,data_freq,numthreads=numthreads,masks=region_masks[model],file_pattern=file_pattern)
				print(experiment,data_masked[model][experiment].keys())

		# write out data
		with open(data_pkl,'wb') as f_pkl:
			pickle.dump(data_masked,f_pkl,-1)
