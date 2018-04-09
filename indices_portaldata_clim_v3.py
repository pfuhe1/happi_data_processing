# Script to do monthly means then ensemble average of HAPPI data
import os,glob,tempfile,shutil,socket
from threading import Thread
import multiprocessing
from get_runs import get_runs
from netCDF4 import MFDataset,Dataset
import numpy as np

# Create list into a string
def list_to_string(l):
	s = ''
	for item in l:
		s += item +' '
	return s

# Create list into a string (also adding monmean operator)
def list_to_string_monmean(l):
	s = ''
	for item in l:
		s += '-monmean '+ item +' '
#		s += item +' '
	return s

def list_to_string_raindays(l,unit_conv):
	thresh = str(1/unit_conv) # Threshold (1mm/day)
	s = ''
	for item in l:
		s += '-expr,"raindays=pr>'+thresh+'" '+ item +' '
	return s[:-1] # Get rid of final space

def create_netcdf_dryspell(template,var,data,outname):
	if type(template)==type(''): # template is a string
		template = Dataset(template,'r') # Read file

	# create outfile object
	outfile=Dataset(outname,'w',format ='NETCDF4_CLASSIC')

	# Create dimensions copied from template file
	temp=template.variables[var]
	newvar = 'dryspell'
	for dim in temp.dimensions:
		if dim=='lat' or dim =='lon' or dim=='time':
			leng=len(template.dimensions[dim])
			if dim == 'time':
				outfile.createDimension(dim,None)
				outfile.createVariable(dim,'d',(dim,))
				outfile.variables[dim][:]=template.variables[dim][:1] # Only make 1 timesteps
			else:
				outfile.createDimension(dim,leng)
				outfile.createVariable(dim,'d',(dim,))
				outfile.variables[dim][:]=template.variables[dim][:]
				
			#print template.variables[dim].__dict__
			for att in template.variables[dim].ncattrs():
				outfile.variables[dim].__setattr__(att,template.variables[dim].__getattribute__(att))

	# Create data variable (named region_mask)
	outfile.createVariable(newvar,'f',['time','lat','lon'])
	for att in temp.ncattrs():
		if not att=='_FillValue':
			outfile.variables[newvar].__setattr__(att,template.variables[var].__getattribute__(att))
	# Override these attributes
	outfile.variables[newvar].__setattr__('long_name',template.variables[var].__getattribute__('long_name')+': consecutive days < 1mm')
	outfile.variables[newvar].__setattr__('units','days')
	# Write data
	outfile.variables[newvar][:]=data
	
	if template.variables.has_key('lat_bnds'):
		outfile.createDimension('bnds',2)
		outfile.createVariable('lat_bnds','d',['lat','bnds'])
		outfile.createVariable('lon_bnds','d',['lon','bnds'])
		outfile.variables['lat_bnds'][:]=template.variables['lat_bnds'][:]
		outfile.variables['lon_bnds'][:]=template.variables['lon_bnds'][:]

#	if template.variables.has_key('time_bnds'):
#		if not 'bnds' in outfile.dimensions:
#			outfile.createDimension('bnds',2)
#		outfile.createVariable('time_bnds','d',['time','bnds'])
#		outfile.variables['time_bnds'][1,:]=template.variables['time_bnds'][:]

	for att in template.ncattrs():
		outfile.__setattr__(att,getattr(template,att))
	outfile.__setattr__('history',getattr(outfile,'history') + '\n Dryspell calculated by process_run_dryspell python function in indices_portaldata_clim_v3.py')
	

	#outfile.flush()
	outfile.close()

def process_run_dryspell(runpath,whole_run,unit_conv):
	if os.path.isdir(runpath):
		# get input files in runpath
		run_files=sorted(glob.glob(runpath+'/*.nc'))
	else:
		# The runpath is the file
		run_files = [runpath]
	run = os.path.basename(runpath)
	print run	

	if len(run_files)==0:
		raise Exception('error, no files found: '+runpath)
	
	with MFDataset(run_files,'r') as f_netcdf:
		precip = f_netcdf.variables['pr'][:]*unit_conv # convert to mm/day
		countdry = np.zeros(precip.shape[1:])
		maxdry = np.zeros(precip.shape[1:])
		noraindays = precip < 1.0
		for t in range(precip.shape[0]):
				countdry[noraindays[t,:]]+=1
				countdry[~noraindays[t,:]]=0
				maxdry = np.maximum(maxdry,countdry)
	newshape = [1]+list(maxdry.shape)
	create_netcdf_dryspell(run_files[0],'pr',maxdry.reshape(newshape),whole_run)

# Process data for a single ensemble member/ run
def process_run_raindays(runpath,run_whole,unit_conv):
	thresh = str(1/unit_conv) # Threshold (1mm/day)
	if os.path.isdir(runpath):
		# get input files in runpath
		run_files=glob.glob(runpath+'/*.nc')
	else:
		# The runpath is the file
		run_files = [runpath]
	run = os.path.basename(runpath)
	print run

	if len(run_files)==0:
		raise Exception('error, no files found: '+runpath)
	elif len(run_files)==1:
		# Only one file, calculate raindays
		# CDO command
		cdo_cmd = "cdo ymonmean -expr,'raindays=pr>"+thresh+"' " + run_files[0] + " " + run_whole
		print cdo_cmd
		os.system(cdo_cmd)
	else: 
		# calculate raindays and concatenate files (have to do in two parts)
		# CDO command
		run_cat = run_whole[:-3]+'_cat.nc'
		cdo_cmd = "cdo cat " + list_to_string_raindays(run_files,unit_conv) +" "+ run_cat
		print cdo_cmd
		os.system(cdo_cmd)
		cdo_cmd2 = "cdo ymonmean " + run_cat + " " + run_whole
		os.system(cdo_cmd2)
		os.remove(run_cat)

# Process data for a single ensemble member/ run
def process_run_yrmax(runpath,run_whole,unit_conv):
	if os.path.isdir(runpath):
		# get input files in runpath
		run_files=glob.glob(runpath+'/*.nc')
	else:
		# The runpath is the file
		run_files = [runpath]
	run = os.path.basename(runpath)
	print run

	if len(run_files)==0:
		raise Exception('error, no files found: '+runpath)
	elif len(run_files)==1:
		# Only one file, calculate yearmax
		# CDO command
		cdo_cmd = "cdo yearmax " + run_files[0] + " " + run_whole
		print cdo_cmd
		os.system(cdo_cmd)
	else: 
		# calculate yearmax and concatenate files
		# CDO command
		cdo_cmd = "cdo yearmax -cat '" +list_to_string(run_files) + "' " + run_whole
		print cdo_cmd
		os.system(cdo_cmd)

# Process data for a single ensemble member/ run
def process_run_Rx5day(runpath,run_whole,unit_conv):
	if os.path.isdir(runpath):
		# get input files in runpath
		run_files=glob.glob(runpath+'/*.nc')
	else:
		# The runpath is the file
		run_files = [runpath]
	run = os.path.basename(runpath)
	print run

	if len(run_files)==0:
		raise Exception('error, no files found: '+runpath)
	elif len(run_files)==1:
		# Only one file, calculate Rx5day (monthly 5 day max)
		# CDO command
		cdo_cmd = "cdo ymonmax -runmean,5 " + run_files[0] + " " + run_whole
		print cdo_cmd
		os.system(cdo_cmd)
	else: 
		# calculate Rx5day (monthly 5 day max) and concatenate files
		# CDO command
		cdo_cmd = "cdo ymonmax -runmean,5 -cat '" +list_to_string(run_files) + "' " + run_whole
		print cdo_cmd
		os.system(cdo_cmd)

# Process data for a single ensemble member/ run
def process_run_RXx5day(runpath,run_whole,unit_conv):
	# Generates 10 yearly mean of RXx5day, and 'yrly' files of RXx5day (yearly max of 5 daily pr)
	if os.path.isdir(runpath):
		# get input files in runpath
		run_files=glob.glob(runpath+'/*.nc')
	else:
		# The runpath is the file
		run_files = [runpath]
	run = os.path.basename(runpath)
	print run

	if len(run_files)==0:
		raise Exception('error, no files found: '+runpath)
	elif len(run_files)==1:
		# Only one file, calculate RXx5day (annual 5 day max)
		# CDO command
		cdo_cmd = "cdo yearmax -runmean,5 " + run_files[0] + " " + run_whole[:-3]+'_yrly.nc'
		print cdo_cmd
		os.system(cdo_cmd)
	else: 
		# calculate RXx5day (annual 5 day max) and concatenate files
		# CDO command
		cdo_cmd = "cdo yearmax -runmean,5 -cat '" +list_to_string(run_files) + "' " + run_whole[:-3]+'_yrly.nc'
		print cdo_cmd
		os.system(cdo_cmd)

	cdo_cmd2 = "cdo timmean "+run_whole[:-3]+'_yrly.nc' + ' ' + run_whole
	print cdo_cmd2
	os.system(cdo_cmd2)

# Process all the data for the particular model, experiment and variable
def process_data(model,experiment,var,index,basepath,numthreads,unit_conv,data_freq = 'day',domain='atmos'):

	try:		
	
		# set specific 'process' function to call
		if index == 'raindays':
			process = process_run_raindays
		elif index == 'Rx5day':
			process = process_run_Rx5day
		elif index == 'RXx5day':
			process = process_run_RXx5day
		elif index == 'dryspell':
			process = process_run_dryspell
		elif index == 'yrmax':
			process = process_run_yrmax
			if data_freq == 'day':
				#rename index
				index = var+'yrmaxdaily'
			if data_freq == 'mon':
				index = var+'yrmaxmonthly'
		
		outpath_runs=os.path.join(outdir,'indices_data',model,experiment,index)
		if not os.path.exists(outpath_runs):
			os.makedirs(outpath_runs)

		if not os.path.exists(outdir+'/indices_ensmean/'):
			os.makedirs(outdir+'/indices_ensmean/')
			
		print model,experiment,var
		outmean = outdir+'/indices_ensmean/'+model+'.'+index+'.'+experiment+'_monclim_ensmean.nc'
		outstd = outdir+'/indices_ensmean/'+model+'.'+index+'.'+experiment+'_monclim_ensstd.nc'
		if os.path.exists(outmean) and os.path.exists(outstd):
			print 'files already exist, skipping'
			return
		
		# Create pool of processes to process runs in parallel. 
		pool = multiprocessing.Pool(processes=numthreads)


		# Loop over runs
		run_averages = ''
		runs = get_runs(model,experiment,basepath,data_freq,var,domain=domain)
		for runpath in runs:
			run = os.path.basename(runpath)
			if os.path.isdir(runpath):
				# Add .nc to the end if run is a dir
				run_whole = os.path.join(outpath_runs,model+'_'+experiment+'_'+index+'_'+run+'.nc')
			else: 
				# Run contains '.nc' already
				run_whole = os.path.join(outpath_runs,model+'_'+experiment+'_'+index+'_'+run)
			# Only process if it doesn't exist
			if not os.path.exists(run_whole):
				#raise Exception('Debug, not processing runs')
				pool.apply_async(process,(runpath,run_whole,unit_conv))
				#process(runpath,run_whole)
			# Add this run to list
			run_averages += run_whole +' '

		# close the pool and make sure the processing has finished
		pool.close()
		pool.join()		

		# Ensemble mean
		cdo_cmd = 'cdo ensmean ' + run_averages + outmean
		print cdo_cmd
		os.system(cdo_cmd)

		# Ensemble stdev
		cdo_cmd = 'cdo ensstd ' + run_averages + outstd
		print cdo_cmd
		os.system(cdo_cmd)

	except Exception,e:
		print 'Error in script: '
		print e
		raise


if __name__=='__main__':

	host=socket.gethostname()
	# CAM5 data is stored and should be processed on triassic
	if host=='triassic.ggy.bris.ac.uk':
		#basepath='/export/triassic/array-01/pu17449/happi_data_decade/'
		#models = ['CAM5-1-2-025degree']
		basepath='/export/triassic/array-01/pu17449/happi_data_monthly/'
		models = ['NorESM1-HAPPI','MIROC5','CanAM4','CAM4-2degree']
		# Number of processes to run in parallel to process ensemble members
		numthreads = 2
	# All other data is stored on silurian
	elif host =='silurian.ggy.bris.ac.uk':
		basepath = '/export/silurian/array-01/pu17449/happi_data/'
		models = ['NorESM1-HAPPI','MIROC5','CanAM4','CAM4-2degree','HadAM3P']
		# Number of processes to run in parallel to process ensemble members
		numthreads = 4
	elif host =='happi.ggy.bris.ac.uk':
		basepath='/data/scratch/happi_data/'
		models = ['NorESM1-HAPPI','MIROC5','CanAM4','CAM4-2degree']
		# Number of processes to run in parallel to process ensemble members
		numthreads = 2	
		outdir = '/data/scratch/pu17449/processed_data/'
	elif host =='anthropocene.ggy.bris.ac.uk':
		basepath = '/export/anthropocene/array-01/pu17449/happi_data/'
		#models = ['NorESM1-HAPPI','MIROC5','CanAM4','CAM4-2degree','HadAM3P']
		models = ['NorESM1-HAPPI','CAM4-2degree','HadAM3P']
		# Number of processes to run in parallel to process ensemble members
		numthreads = 4
		outdir = '/export/anthropocene/array-01/pu17449/happi_processed'


	experiments = ['All-Hist','Plus15-Future','Plus20-Future']
	var = 'mrro'
	domain = 'land'
	data_freq = 'mon'

	#indices = ['raindays','Rx5day','RXx5day','dryspell']
	indices = ['yrmax']
	unit_conv = 86400.

	# PROCESS CESM low warming
	#models = ['CESM-CAM5']
#	# override defaults
	#basepath = '/export/silurian/array-01/pu17449/CESM_low_warming/decade_data_v2/'
	#basepath = '/export/anthropocene/array-01/pu17449/cesm_data/decade_data_v2/'
	#experiments = ['historical','1pt5degC','2pt0degC']
#	outdir = '/export/silurian/array-01/pu17449/CESM_low_warming/indices/'
#	unit_conv = 86400.*1000 # Note, not needed for runoff

	# PROCESS CMIP5 slices
#	models = ['CMIP5']
#	experiments = ['historical','slice15','slice20']
#	basepath = '/export/silurian/array-01/pu17449/CMIP5_slices/subset_daily_regrid'
#	outdir = '/export/silurian/array-01/pu17449/CMIP5_slices/indices_daily_regrid/'
#	basepath = '/export/silurian/array-01/pu17449/CMIP5_slices/subset_daily'
#	outdir = '/export/silurian/array-01/pu17449/CMIP5_slices/indices_daily'


	for model in models:
		for experiment in experiments:
			for index in indices:
				# Call process_data for this model, experiment and variable
				process_data(model,experiment,var,index,basepath,numthreads,unit_conv,data_freq=data_freq,domain=domain)
				# Simple Multithreading to call process_data in separate threads
	#			t = Thread(target = process_data, args=(model,experiment,var,basepath))
	#			t.start()

