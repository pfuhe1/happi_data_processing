# Script to do monthly means then ensemble average of HAPPI data
import os,glob,tempfile,shutil
from threading import Thread
from netCDF4 import num2date,Dataset,MFDataset
import matplotlib.pyplot as plt
from mpl_toolkits.basemap import Basemap,cm
import multiprocessing
import numpy as np
import pickle

# Create list into a string (also adding monmean operator)
def list_to_string_monmean(l):
	s = ''
	for item in l:
		s += '-monmean '+ item +' '
#		s += item +' '
	return s

# Create list into a string (also adding raindays operator)
def list_to_string_raindays(l):
	s = ''
	for item in l:
		s += '-expr,"raindays=pr>0.000011574" '+ item +' '
	return s[:-1] # Get rid of final space
# Create list into a string 
def list_to_string(l):
	s = ''
	for item in l:
		s += item +' '
	return s

# Choose runs 1-50 and 111-160 for MIROC Hist
def miroc_histruns(basepath,model,experiment,var):
	runs = []
	for run in range(1,51):
		runs.append(basepath+model+'/'+experiment+ '/est1/v2-0/day/atmos/'+ var+'/run'+str(run).zfill(3))
	for run in range(111,161):
		runs.append(basepath+model+'/'+experiment+ '/est1/v2-0/day/atmos/'+ var+'/run'+str(run).zfill(3))
	return runs

# Choose runs 1-125 for NorESM1-HAPPI Hist
def norESM_histruns(basepath,model,experiment,var):
	runs = []
	for run in range(1,126):
		runs.append(basepath+model+'/'+experiment+ '/est1/v1-0/day/atmos/'+ var+'/run'+str(run).zfill(3))
	return runs

# Get list of runs for a particular model, experiment, variable
def get_runs(model,experiment,var,basepath):
	run_pattern = None
	if model=='MIROC5' and experiment=='All-Hist':
		# choose specific runs
		runs = miroc_histruns(basepath,model,experiment,var)
	elif model=='NorESM1-HAPPI' and experiment=='All-Hist':
		# choose specific runs
		runs = norESM_histruns(basepath,model,experiment,var)
	elif model=='MIROC5' or model=='NorESM1-HAPPI': 
		# For other scenarios choose all runs
		run_pattern = 'run*'
	elif model=='CAM4-2degree':
		# For CAM4-2degree choose first 500 or so (runs from 1000 are longer for bias correction)
		run_pattern = 'ens0*'
	elif model=='CanAM4':
		run_pattern = 'r*i1p1'
	if run_pattern:
		pathpattern=basepath+model+'/'+experiment+ '/*/*/day/atmos/'+ var+'/'+run_pattern
		runs = glob.glob(pathpattern)
	return runs

def rainfall_indices(runpath):
	run = os.path.basename(runpath)
	print run
	files = glob.glob(runpath+'/*.nc')
	with MFDataset(files,'r') as f_netcdf:
		precip = f_netcdf.variables['pr'][:]*86400. # convert to mm/day
		noraindays = precip < 1.0

		# Mask precip for only rainy days
		# rainfall intensity index is mean over rainy days
		rain_masked = np.ma.masked_where(noraindays,precip)
		spii = rain_masked.mean(0)

		# Calculat longest dry spell
		countdry = np.zeros(precip.shape[1:])
		maxdry = np.zeros(precip.shape[1:])
		for t in range(precip.shape[0]):
			countdry[noraindays[t,:]]+=1
			countdry[~noraindays[t,:]]=0
			maxdry = np.maximum(maxdry,countdry)
	
	return spii,maxdry

def rainfall_intensity_index(runpath):
	run = os.path.basename(runpath)
	print run
	files = glob.glob(runpath+'/*.nc')
	with MFDataset(files,'r') as f_netcdf:
		precip = f_netcdf.variables['pr'][:]*86400. # convert to mm/day
		noraindays = precip < 1.0
		rain_masked = np.ma.masked_where(noraindays,precip)
		
		return rain_masked.mean(0)

def dry_spell_index(runpath):
	run = os.path.basename(runpath)
	print run
	files = glob.glob(runpath+'/*.nc')
	with MFDataset(files,'r') as f_netcdf:
		precip = f_netcdf.variables['pr'][:]*86400. # convert to mm/day
		countdry = np.zeros(precip.shape[1:])
		maxdry = np.zeros(precip.shape[1:])
		noraindays = precip < 1.0
		for t in range(precip.shape[0]):
			countdry[noraindays[t,:]]+=1
			countdry[~noraindays[t,:]]=0
			maxdry = np.maximum(maxdry,countdry)
			
			
	return maxdry
		

# Process all the data for the particular model, experiment and variable
def process_data(model,experiment,var,basepath,numthreads):

	try:		
		print model,experiment,var

		
		# Create pool of processes to process runs in parallel. 
		pool = multiprocessing.Pool(processes=numthreads)


		# Loop over runs
		runs = get_runs(model,experiment,var,basepath)
		result_all = ['']*len(runs)

		for i,runpath in enumerate(runs):
			result_all[i] = pool.apply_async(rainfall_indices,(runpath,))
			#result_all[i] = rainfall_indices(runpath)

		# close the pool and make sure the processing has finished

		pool.close()
		pool.join()	

		print 'collecting data...'
		data_spii = []
		data_dryspell=[]
		for result in result_all:
#		for tmp in result_all:
			tmp = result.get(timeout=600.)
			data_spii.append(tmp[0])
			data_dryspell.append(tmp[1])

		# Calculate ensemble average
		data_spii=np.array(data_spii).mean(0).squeeze()
		data_dryspell = np.array(data_dryspell).mean(0).squeeze()

		return data_spii,data_dryspell

	except Exception,e:
		print 'Error in script: '
		print e
		raise
		
if __name__=='__main__':

	basepath = '/export/silurian/array-01/pu17449/happi_data/'
	models = ['NorESM1-HAPPI','MIROC5','CanAM4','CAM4-2degree']
	experiments = ['All-Hist','Plus15-Future','Plus20-Future']
	var = 'pr' # Get raindays from precip > 1mm
	numthreads = 8

	lats={}
	lons={}

	f_pkl1 = '/export/silurian/array-01/pu17449/pkl_data/SPII.pkl'
	f_pkl2 = '/export/silurian/array-01/pu17449/pkl_data/dryspell.pkl'
	if os.path.exists(f_pkl1):
		with open(f_pkl1,'rb') as fdata:
			spii_data=pickle.load(fdata)
	else:
		spii_data = {}
	if os.path.exists(f_pkl2):
		with open(f_pkl2,'rb') as fdata:
			dryspell_data=pickle.load(fdata)
	else:
		dryspell_data = {}	


	for model in models:
		print model
		# Initialize data
		if not spii_data.has_key(model):
			spii_data[model]={}
		if not dryspell_data.has_key(model):
			dryspell_data[model]={}
		# Load lat and lon from template file
		template_file = '/export/silurian/array-01/pu17449/processed_data_clim/'+model+'.tas.All-Hist_monclim_ensmean.nc'
		with Dataset(template_file,'r') as ftmp:
			lat = ftmp.variables['lat'][:]
			lon = ftmp.variables['lon'][:]
			lats[model]=lat
			lons[model]=lon

		# loop over expeiments and process data
		for experiment in experiments:
			if not spii_data[model].has_key(experiment) or not dryspell_data[model].has_key(experiment):
				# Call process_data for this model, experiment and variable
				tmp1,tmp2=process_data(model,experiment,var,basepath,numthreads)
				spii_data[model][experiment]=tmp1
				dryspell_data[model][experiment]=tmp2
			# Simple Multithreading to call process_data in separate threads
#			t = Thread(target = process_data, args=(model,experiment,var,basepath))
#			t.start()


		with open(f_pkl1,'wb') as fdata:
			pickle.dump(spii_data,fdata,-1)
			pickle.dump(lats,fdata,-1)
			pickle.dump(lons,fdata,-1)
		with open(f_pkl2,'wb') as fdata:
			pickle.dump(dryspell_data,fdata,-1)
			pickle.dump(lats,fdata,-1)
			pickle.dump(lons,fdata,-1)

		# Set up basemap projection
		m = Basemap(projection='robin',lon_0=180,resolution='c')
		lon2,lat2=np.meshgrid(lon,lat)
		x,y=m(lon2,lat2)
		plt.set_cmap('coolwarm')
		t1size=10

		plt.figure()
		plt.subplot(131)
		plt.title('Hist ensmean',fontsize=t1size)
		print 'datashape',dryspell_data[model]['All-Hist'].shape
		c=m.pcolor(x,y,dryspell_data[model]['All-Hist'],vmin=0,vmax=200)
		m.drawcoastlines()
		plt.colorbar(shrink=0.5)

		plt.subplot(132)
		plt.title('1P5 - Hist ensmean',fontsize=t1size)
		c=m.pcolor(x,y,dryspell_data[model]['Plus15-Future'] - dryspell_data[model]['All-Hist'],vmin=-10,vmax=10)
		m.drawcoastlines()
		plt.colorbar(shrink=0.5)

		plt.subplot(133)
		plt.title('2P0 - 1P5 ensmean',fontsize=t1size)
		c=m.pcolor(x,y,dryspell_data[model]['Plus20-Future'] - dryspell_data[model]['Plus15-Future'],vmin=-10,vmax=10)
		m.drawcoastlines()
		plt.colorbar(shrink=0.5)

		# Finish up plot
		#plt.suptitle(model + ' Rainfall intensity index')
		plt.suptitle(model + ' Dryspell duration index')		
		plt.tight_layout()	
		#plt.show()
		plt.savefig('figs/'+model +'_dryspell.png')
