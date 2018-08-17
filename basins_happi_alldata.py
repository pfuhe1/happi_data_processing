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

# Stuff to load runs
sys.path.append('/home/bridge/pu17449/src/happi_data_processing')
from get_runs import get_runs,get_bc_runs

sys.path.append('/home/bridge/pu17449/src/python_tutorial_v1.1')
from globalmean import calc_globalmean


# Create list into a string 
def list_to_string(l):
	s = ''
	for item in l:
		s += item +' '
	return s

def load_basin_data_weighted(runpath,model,experiment,var,basin_masks,lat):
	print runpath
	basin_timeseries = {}
	if os.path.isdir(runpath):
		run_files=glob.glob(runpath+'/*.nc')
	else:
		run_files = runpath
	
	print run_files
	
	# Hack for issues with last year of MIROC data (11 years)
	# Note: this works for raw data files, but not for indices!
	#if model == 'MIROC5':# and (experiment=='All-Hist' or experiment == 'Plus20-Future'):
	#	run_files = run_files[:-1]

	# Load data from file into data_all array
	with MFDataset(run_files,'r') as f_in:
		data = f_in.variables[var][:].squeeze()*86400
	shp = data.shape
	for bname,basin in basin_masks.iteritems():
		
		# Apply basin mask to array (broadcast over time dimension)
		masked_data = np.ma.masked_array(*np.broadcast_arrays(data,basin))
		# Debug to check mask
		#plt.figure()
		#plt.title('mask '+model)
		#plt.contourf(lon,lat,masked_data[5,:])
		#plt.colorbar()
		#plt.show()
		#if len(shp)==2:
		#	basin_timeseries[bname] = masked_data.mean(0).mean(0)
		#else:
		basin_timeseries[bname] = calc_globalmean(masked_data,lat)

	return basin_timeseries

def load_basin_data(runpath,model,experiment,var,basin_masks,lat):
	print runpath
	basin_timeseries = {}
	if os.path.isdir(runpath):
		run_files=glob.glob(runpath+'/*.nc')
	else:
		run_files = runpath
	
	print run_files
	
	# Hack for issues with last year of MIROC data (11 years)
	# Note: this works for raw data files, but not for indices!
	#if model == 'MIROC5':# and (experiment=='All-Hist' or experiment == 'Plus20-Future'):
	#	run_files = run_files[:-1]

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
		if len(shp)==2:
			basin_timeseries[bname] = masked_data.mean(0).mean(0)
		else:
			basin_timeseries[bname] = masked_data.mean(1).mean(1)

	return basin_timeseries


# Process all the data for the particular model, experiment and variable
def get_basindata(model,experiment,var,basepath,data_freq,numthreads=1,masks=None,file_pattern=None,domain='atmos',getruns=get_runs):

	try:
		# Create pool of processes to process runs in parallel. 
		pool = multiprocessing.Pool(processes=numthreads)

		# Get list of runs
		if file_pattern is None:
			print domain
			runs = getruns(model,experiment,basepath,data_freq,var,domain=domain)
		else:
			fpath=os.path.join(basepath,file_pattern)
			print fpath
			runs = glob.glob(fpath)
		
		# Load grid information
		if os.path.isdir(runs[0]):
			f_template = glob.glob(runs[0]+'/*.nc')[0]
		else:
			f_template = runs[0]
		#print f_template
		with Dataset(f_template,'r') as tmp:
			try:
				lat = tmp.variables['lat'][:]
				lon = tmp.variables['lon'][:]
			except:
				lat = tmp.variables['latitude0'][:]
				lon = tmp.variables['longitude0'][:]
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
		print 'loading data...'
		for i,runpath in enumerate(runs):
			run = os.path.basename(runpath)
			# Add the process to the pool
			result_all[i]  = pool.apply_async(load_basin_data_weighted,(runpath,model,experiment,var,masks,lat))
			# option when not using multithreading pool:
			#result_all[i] = load_basin_data(runpath,model,experiment,var,masks)

		# close the pool and make sure the processing has finished
		pool.close()
		pool.join()

		print '\ncollecting data...'
		data_all = {}
#		for tmp in result_all: # option when not using multithreading
		for result in result_all: # comment out if not using multithreading pool
			tmp = result.get(timeout=600.) # comment out if not using multithreading pool
			# Put data from each basin into different list
			for basin,data in tmp.iteritems():
				if data_all.has_key(basin):
					data_all[basin].append(data)
				else:
					data_all[basin] = [data]

		# convert lists to arrays
		print 'finalising data'
		for basin in masks.keys():
			outshape0 = len(data_all[basin])
			outshape1 = len(data_all[basin][3]) # Hack to work for CAM5, TODO change to maximum of these values (or just 10)
			outarr = np.ma.zeros([outshape0,outshape1])
			for i,ens in enumerate(data_all[basin]):
				#print 'ens',i,len(data_all[basin][i])
				outarr[i,:len(ens)]=ens
				outarr[i,len(ens):]=np.ma.masked # mask away parts allocated for shorter enembles
			data_all[basin] = outarr

		print len(data_all[basin])#,len(data_all[basin][0])#,len(data_all[basin][1]),len(data_all[basin][2]),len(data_all[basin][3]),len(data_all[basin][4]),len(data_all[basin][5]),
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


