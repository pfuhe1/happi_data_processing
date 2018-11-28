#!/usr/bin/env python
# Script to plot out global maps of tasmax
# Peter Uhe
# 24/4/2017

import numpy as np
from netCDF4 import Dataset,MFDataset,num2date
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import matplotlib.gridspec as gridspec
import matplotlib.colors
import pickle
import glob,os,socket,sys
from mpl_toolkits.basemap import Basemap,cm,maskoceans
from datetime import datetime
import multiprocessing
import pickle
from scipy.stats import ks_2samp
import scipy.stats

home = os.environ.get('HOME')
# Stuff to load masks
sys.path.append(os.path.join(home,'src/create_masks'))
from create_mask import load_shapefile_attrs

sys.path.append(os.path.join(home,'src/happi_data_processing'))
from basins_happi_alldata import get_basindata,create_mask



if __name__=='__main__':
	
#######################################
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
		shapefile = '/export/anthropocene/array-01/pu17449/shapefiles/referenceRegions.shp'
		landmask_dir = '/export/anthropocene/array-01/pu17449/happi_data/landmask'
		basepath = '/export/anthropocene/array-01/pu17449/happi_processed/'
		data_pkl = '/export/anthropocene/array-01/pu17449/pkl/RXx5day_IPCCreg_data.pkl'
		mask_pkl = '/export/anthropocene/array-01/pu17449/pkl/IPCCreg_masks.pkl'
		models = ['NorESM1-HAPPI','MIROC5','CanAM4','CAM4-2degree','HadAM3P','CESM-CAM5']
		numthreads = 12
	elif host[:6] == 'jasmin' or host[-11:] == 'jc.rl.ac.uk':
		basepath = '/work/scratch/pfu599/helix_data/processed_data/'
		data_pkl = '/work/scratch/pfu599/pkl/RXx5day_basindata.pkl'
		basin_path = os.path.join(home,'src/happi_analysis/river_basins/basin_files/')
		mask_pkl = '/work/scratch/pfu599/pkl/basin_masks.pkl'
		numthreads = 1
		#models = ['CESM-CAM5','NorESM1-HAPPI','MIROC5','CanAM4','CAM4-2degree','HadAM3P','ECHAM6-3-LR','CAM5-1-2-025degree','ec-earth3-hr','hadgem3']
		models = ['ec-earth3-hr','hadgem3']
		

	# Test CMIP5:
	#models = ['CMIP5']

	data_freq = 'N/A'
	var = 'pr'
	index = 'RXx5day'



	if os.path.exists(data_pkl):
		with open(data_pkl,'rb') as f_pkl:
			data_masked = pickle.load(f_pkl)
	else:
		data_masked = {}

	if os.path.exists(mask_pkl):
		with open(mask_pkl,'rb') as f_pkl:
			basin_masks = pickle.load(f_pkl)
	else:
		basin_masks = {}

	# Convert precip to mm/day 
	unit_scale = 86400.
	unit_add = 0.

	print 'loading region polygons...'
	reg_polygons,reg_attrs = load_shapefile_attrs(shapefile,'LAB')


###########################################################################################

	for model in models:
		print model

		# Load grid information
		if model =='CESM-CAM5':
			experiments = ['historical','1pt5degC','2pt0degC']
		elif model == 'CMIP5':
			experiments = ['historical','slice15','slice20']
		else:
			experiments = ['All-Hist','Plus15-Future','Plus20-Future']

		basepath2 = os.path.join(basepath,'indices_data',model)
		#f_template = os.path.join(basepath,'indices_ensmean',model+'.'+index+'.'+experiments[0]+'_monclim_ensmean.nc')
		print landmask_dir+'/sftlf_fx_'+model+'_*.nc'
		f_landmask = glob.glob(landmask_dir+'/sftlf_fx_'+model+'_*.nc')[0]
#		f_template = os.path.join(basepath,'../happi_processed/indices_ensmean',model+'.'+index+'.'+experiments[0]+'_monclim_ensmean.nc')

		print f_landmask
		with Dataset(f_landmask,'r') as tmp:
			lat = tmp.variables['lat'][:]
			lon = tmp.variables['lon'][:]
			oceanpoints = tmp.variables['sftlf'][:]<1. # Land fractionless than 1% (any land treat as land point)
			
		# Create 2D arrays of lon and lat
		nlat = len(lat)
		nlon = len(lon)
		lonxx,latyy=np.meshgrid(lon,lat)
		lonxx[lonxx>180]=lonxx[lonxx>180]-360
		points = np.vstack((lonxx.flatten(),latyy.flatten())).T
		
		# Create masks
		print 'Creating masks from polygons'
		pool = multiprocessing.Pool(processes=len(reg_polygons.keys()))
		if not basin_masks.has_key(model):
			basin_masks[model]={}
		# Hack to delete saved mask for this model
		#basin_masks['HadAM3P']={}
		results = {}
		for basin,polygons in reg_polygons.iteritems():
			if not basin_masks[model].has_key(basin):
				results[basin]=pool.apply_async(create_mask,(polygons,points,nlat,nlon))
		# close the pool and make sure the processing has finished
		pool.close()
		pool.join()
		print 'collecting data...'
		for basin,result in results.iteritems():
			polymask = result.get(timeout=60.)
			use = reg_attrs[basin]['USAGE']
			if use == 'all':
				basin_masks[model][basin[:3]] = polymask
			elif use == 'land': # Mask out ocean points
				basin_masks[model][basin[:3]] = np.logical_or(polymask,oceanpoints)
			elif use == 'land; sea': # create separate masks for land a sea for this region
				basin_masks[model][basin[:3]+'_land']=np.logical_or(polymask,np.logical_not(oceanpoints))
				basin_masks[model][basin[:3]+'_sea']=np.logical_or(polymask,oceanpoints)
			

		# write out masks
		with open(mask_pkl,'wb') as f_pkl:
			pickle.dump(basin_masks,f_pkl,-1)

		# Load data
		if not data_masked.has_key(model):
			data_masked[model] = {}
		for experiment in experiments:
			file_pattern = experiment+'/'+index+'/*_yrly.nc'
			if not data_masked[model].has_key(experiment):
				data_masked[model][experiment] = get_basindata(model,experiment,var,basepath2,data_freq,numthreads=numthreads,masks=basin_masks[model],file_pattern=file_pattern)
				print experiment,data_masked[model][experiment].keys()

		# write out data
		with open(data_pkl,'wb') as f_pkl:
			pickle.dump(data_masked,f_pkl,-1)

