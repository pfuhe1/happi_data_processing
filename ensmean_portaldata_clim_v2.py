# Script to do monthly means then ensemble average of HAPPI data
import os,glob,tempfile,shutil
import multiprocessing,time
import socket


# Create list into a string 
def list_to_string(l):
	s = ''
	for item in l:
		s += item +' '
	return s

# Choose runs 1-50 and 111-160 for MIROC Hist
def miroc_histruns(basepath):
	runs = []
	for run in range(1,51):
		runs.append(basepath+model+'/'+experiment+ '/est1/v2-0/mon/atmos/'+ var+'/run'+str(run).zfill(3))
	for run in range(111,161):
		runs.append(basepath+model+'/'+experiment+ '/est1/v2-0/mon/atmos/'+ var+'/run'+str(run).zfill(3))
	return runs

# Choose runs 1-125 for NorESM1-HAPPI Hist
def norESM_histruns(basepath):
	runs = []
	for run in range(1,126):
		runs.append(basepath+model+'/'+experiment+ '/est1/v1-0/mon/atmos/'+ var+'/run'+str(run).zfill(3))
	return runs

# Get list of runs for a particular model, experiment, variable
def get_runs(model,experiment,basepath,data_freq):
	run_pattern = None
	if model=='MIROC5' and experiment=='All-Hist':
		# choose specific runs
		runs = miroc_histruns(basepath)
	elif model=='NorESM1-HAPPI' and experiment=='All-Hist':
		# choose specific runs
		runs = norESM_histruns(basepath)
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

# Process data for a single ensemble member/ run
def process_run(runpath,run_whole,operator='mean'):
	run = os.path.basename(runpath)
	print run
	run_files=glob.glob(runpath+'/*.nc')
	if len(run_files)==0:
		raise Exception('error, no files found: '+fnames)
	elif len(run_files)==1:
		# Only one file
		# CDO command
		cdo_cmd = 'cdo ymon'+operator+' ' + run_files[0] + ' ' + run_whole
		print cdo_cmd
		os.system(cdo_cmd)
	else: 
		# concatenate files
		# CDO command
		cdo_cmd = "cdo ymon"+operator+"  -cat '" + list_to_string(run_files) + "' "+ run_whole
		print cdo_cmd
		os.system(cdo_cmd)


# Process all the data for the particular model, experiment and variable
def process_data(model,experiment,var,basepath,numthreads,data_freq):
	try:		
		print model,experiment,var
		clim_ensmean = '/export/silurian/array-01/pu17449/processed_data_clim/'+model+'.'+var+'.'+experiment+'_monclim_ensmean.nc'
		clim_ensstd = '/export/silurian/array-01/pu17449/processed_data_clim/'+model+'.'+var+'.'+experiment+'_monclim_ensstd.nc'
		if data_freq=='day':
			std_ensmean = '/export/silurian/array-01/pu17449/processed_data_clim/'+model+'.'+var+'.'+experiment+'_monstd_ensmean.nc'
			std_ensstd = '/export/silurian/array-01/pu17449/processed_data_clim/'+model+'.'+var+'.'+experiment+'_monstd_ensstd.nc'

		if os.path.exists(clim_ensmean) and os.path.exists(clim_ensstd):
			print 'files already exist, skipping'

			return
		# Create pool of processes to process runs in parallel. 
		pool = multiprocessing.Pool(processes=numthreads)

		# Loop over runs
		run_averages = ''
		run_averages2 = ''
		runs = get_runs(model,experiment,basepath,data_freq)

		outpath_runs=os.path.join(basepath,'monclim_data',model,experiment,var)
		if not os.path.exists(outpath_runs):
			os.makedirs(outpath_runs)

		for runpath in runs:
			run = os.path.basename(runpath)
			# Calculate ymonmean
			run_whole = os.path.join(outpath_runs,model+'_'+experiment+'_'+var+'_'+run+'_clim.nc')
			# Add the process to the pool
			if not os.path.exists(run_whole):
				pool.apply_async(process_run,(runpath,run_whole))
			# Add this run to list
			run_averages += run_whole +' '
			# for daily data calculate ymonstd
			if data_freq == 'day':
				run_whole_std = run_whole[:-7]+'std.nc'
				if not os.path.exists(run_whole_std):
					pool.apply_async(process_run,(runpath,run_whole_std),{'operator':'std'})
				run_averages2 += run_whole_std +' '


		# close the pool and make sure the processing has finished
		pool.close()
		pool.join()

		# Ensemble mean
		cdo_cmd = 'cdo ensmean ' + run_averages + clim_ensmean
		print cdo_cmd
		os.system(cdo_cmd)

		# Ensemble stdev
		cdo_cmd = 'cdo ensstd ' + run_averages + clim_ensstd
		print cdo_cmd
		os.system(cdo_cmd)

		# Daily data also do standard deviation
		if data_freq=='day':
			# Ensemble mean
			cdo_cmd = 'cdo ensmean ' + run_averages2 + std_ensmean
			print cdo_cmd
			os.system(cdo_cmd)

			# Ensemble stdev
			cdo_cmd = 'cdo ensstd ' + run_averages2 + std_ensstd
			print cdo_cmd
			os.system(cdo_cmd)

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
	varlist = ['pr','tasmin','tasmax','rsds','tas']
	data_freq = {'pr':'day','tasmin':'day','tasmax':'day','tas':'mon','rsds':'mon'}



	for model in models:
		# Exception to the data_freq list above is HadAM3P which has daily 'rsds' and 'tas'
		if model == 'HadAM3P':
			data_freq['rsds']='day'
			data_freq['tas']='day'
		for experiment in experiments:
			for var in varlist:
				process_data(model,experiment,var,basepath,numthreads,data_freq[var])


