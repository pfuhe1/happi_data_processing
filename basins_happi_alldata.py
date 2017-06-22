# Script to do monthly means then ensemble average of HAPPI data
import os,glob,tempfile,shutil
import multiprocessing,time
import socket,sys
import matplotlib.pyplot as plt
import numpy as np
from netCDF4 import Dataset,MFDataset

# Stuff to load masks
sys.path.append('/home/bridge/pu17449/src/happi_analysis/river_basins')
from create_mask import create_mask,load_polygons


# Create list into a string 
def list_to_string(l):
	s = ''
	for item in l:
		s += item +' '
	return s

# Choose runs 1-50 and 111-160 for MIROC Hist
def miroc_histruns(basepath,model,experiment,var,data_freq):
	runs = []
	for run in range(1,51):
		runs.append(basepath+model+'/'+experiment+ '/est1/v2-0/'+data_freq+'/atmos/'+ var+'/run'+str(run).zfill(3))
	for run in range(111,161):
		runs.append(basepath+model+'/'+experiment+ '/est1/v2-0/'+data_freq+'/atmos/'+ var+'/run'+str(run).zfill(3))
	return runs

# Choose runs 1-125 for NorESM1-HAPPI Hist
def norESM_histruns(basepath,model,experiment,var,data_freq):
	runs = []
	for run in range(1,126):
		runs.append(basepath+model+'/'+experiment+ '/est1/v1-0/'+data_freq+'/atmos/'+ var+'/run'+str(run).zfill(3))
	return runs

# Choose runs from v1-0 (not v1-0-aero) for CAM5-1-2-025 Hist
def CAM5_histruns(basepath,model,experiment,var,data_freq):
	runs =glob.glob(basepath+model+'/'+experiment+ '/est1/v1-0/'+data_freq+'/atmos/'+ var+'/run*')
	return runs

# Get list of runs for a particular model, experiment, variable
def get_runs(model,experiment,basepath,data_freq,var):
	run_pattern = None
	if model=='MIROC5' and experiment=='All-Hist':
		# choose specific runs
		runs = miroc_histruns(basepath,model,experiment,var,data_freq)
	elif model=='NorESM1-HAPPI' and experiment=='All-Hist':
		# choose specific runs
		runs = norESM_histruns(basepath,model,experiment,var,data_freq)
	elif model == 'CAM5-1-2-025degree' and experiment == 'All-Hist':
		runs = CAM5_histruns(basepath,model,experiment,var,data_freq)
	elif model=='MIROC5' or model=='NorESM1-HAPPI' or model=='HadAM3P' or model=='CAM5-1-2-025degree': 
		# For other scenarios choose all runs
		run_pattern = 'run*'
	elif model=='CAM4-2degree':
		run_pattern = 'ens0*'
	elif model=='CanAM4':
		run_pattern = 'r*i1p1'
	if run_pattern:
		pathpattern=basepath+model+'/'+experiment+ '/*/*/'+data_freq+'/atmos/'+ var+'/'+run_pattern
		print pathpattern
		runs = glob.glob(pathpattern)
	return runs

def load_basin_data(runpath,var,basin_masks):
	#print runpath
	basin_timeseries = {}
	# Hack to only take input files from 2000's
	run_files=glob.glob(runpath+'/*_2*-*.nc')

	# Load data from file into data_all array
	with MFDataset(run_files,'r') as f_in:
		data = f_in.variables[var][:].squeeze()*86400
	shp = data.shape
	for bname,basin in basin_masks.iteritems():
		#print bname
		#print 'shape',shp,basin.shape
		# Apply basin mask to array (broadcast over time dimension)
		masked_data = np.ma.masked_array(*np.broadcast_arrays(data,basin))
		# Debug to check mask
		#plt.figure()
		#plt.contourf(masked_data[5,:])
		#plt.colorbar()
		#plt.show()
		basin_timeseries[bname] = masked_data.mean(1).mean(1)

	return basin_timeseries
		

# Process all the data for the particular model, experiment and variable
def get_basindata(model,experiment,var,basepath,data_freq,numthreads=1,masks=None):

	try:
		# Create pool of processes to process runs in parallel. 
		pool = multiprocessing.Pool(processes=numthreads)

		# Get list of runs
		runs = get_runs(model,experiment,basepath,data_freq,var)
		data_all = ['']*len(runs)

		# Load grid information
		f_template = glob.glob(runs[0]+'/*.nc')[0]
		with Dataset(f_template,'r') as tmp:
			lat = tmp.variables['lat'][:]
			lon = tmp.variables['lon'][:]
		# Create 2D arrays of lon and lat
		nlat = len(lat)
		nlon = len(lon)
		lonxx,latyy=np.meshgrid(lon,lat)
		lonxx[lonxx>180]=lonxx[lonxx>180]-360
		points = np.vstack((lonxx.flatten(),latyy.flatten())).T
		
		# Load basin masks
		if masks == None:
			masks = {}
			print 'loading basin masks...'
			for basin_file in glob.glob('/home/bridge/pu17449/src/happi_analysis/river_basins/basin_files/*.txt'):
				polygons = load_polygons(basin_file)
				basin_name = os.path.basename(basin_file)[:-10].replace('_',' ')
				print basin_name
				masks[basin_name]=create_mask(polygons,points,nlat,nlon)

		result_all = ['']*len(runs)

		# Loop over runs
		for i,runpath in enumerate(runs):
			run = os.path.basename(runpath)
			
			# Add the process to the pool
			#result_all[i]  = pool.apply_async(load_basin_data,(runpath,var,masks))
			result_all[i] = load_basin_data(runpath,var,masks)

		# close the pool and make sure the processing has finished
		pool.close()
		pool.join()

		print 'collecting data...'
		data_all = {}
#		for result in result_all:
		for tmp in result_all:
#			tmp = result.get(timeout=600.)
			# Put data from each basin into different list
			for basin,data in tmp.iteritems():
				if data_all.has_key(basin):
					data_all[basin].append(data)
				else:
					data_all[basin] = [data]

		# convert lists to arrays
		print 'finalising data'
		for basin in masks.keys():
			print len(data_all[basin]),len(data_all[basin][0])
			data_all[basin] = np.array(data_all[basin])
			print data_all[basin].shape
		return data_all

	except Exception,e:
		print 'Error in script: '
		print e
		raise

if __name__=='__main__':

	host=socket.gethostname()
	if host=='triassic.ggy.bris.ac.uk':
		basepath='/export/triassic/array-01/pu17449/happi_data2/'
		models = ['CAM5-1-2-025degree']
		# Number of processes to run in parallel to process ensemble members
		numthreads = 5
	elif host =='silurian.ggy.bris.ac.uk':
		basepath = '/export/silurian/array-01/pu17449/happi_data/'
		models = ['NorESM1-HAPPI','MIROC5','CanAM4','CAM4-2degree','HadAM3P']
		# Number of processes to run in parallel to process ensemble members
		numthreads = 12

	experiments = ['All-Hist','Plus15-Future','Plus20-Future']

	var = 'pr'
	data_freq = 'mon'
	basin_data = {}

	for model in models:
		basin_data[model]={}
		for experiment in experiments:
			basin_data[model][experiment] = get_basindata(model,experiment,var,basepath,data_freq,numthreads)

