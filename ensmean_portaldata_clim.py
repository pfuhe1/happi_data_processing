# Script to do monthly means then ensemble average of HAPPI data
import os,glob,tempfile,shutil
import multiprocessing,time


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
def get_runs(model,experiment,basepath):
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
		pathpattern=basepath+model+'/'+experiment+ '/*/*/mon/atmos/'+ var+'/'+run_pattern
		print pathpattern
		runs = glob.glob(pathpattern)
	return runs

# Process data for a single ensemble member/ run
def process_run(runpath,run_whole):
	run = os.path.basename(runpath)
	print run
	run_files=glob.glob(runpath+'/*.nc')
	if len(run_files)==0:
		raise Exception('error, no files found: '+fnames)
	elif len(run_files)==1:
		# Only one file
		# CDO command
		cdo_cmd = 'cdo ymonmean ' + run_files[0] + ' ' + run_whole
		print cdo_cmd
		os.system(cdo_cmd)
	else: 
		# concatenate files
		# CDO command
		cdo_cmd = "cdo ymonmean -cat '" + list_to_string(run_files) + "' "+ run_whole
		print cdo_cmd
		os.system(cdo_cmd)

# Process all the data for the particular model, experiment and variable
def process_data(model,experiment,var,basepath,numthreads):
	temp_dir = tempfile.mkdtemp(dir=basepath+'/../')
	try:		
		print model,experiment,var
		outmean = '/export/silurian/array-01/pu17449/processed_data_clim/'+model+'.'+var+'.'+experiment+'_monclim_ensmean.nc'
		outstd = '/export/silurian/array-01/pu17449/processed_data_clim/'+model+'.'+var+'.'+experiment+'_monclim_ensstd.nc'
		if os.path.exists(outmean) and os.path.exists(outstd):
			print 'files already exist, skipping'

			return
		# Create pool of processes to process runs in parallel. 
		pool = multiprocessing.Pool(processes=numthreads)

		# Loop over runs
		run_averages = ''
		runs = get_runs(model,experiment,basepath)
		for runpath in runs:
			run = os.path.basename(runpath)
			run_whole = os.path.join(temp_dir,model+'_'+experiment+'_'+var+'_'+run+'.nc')
			# Add the process to the pool
			pool.apply_async(process_run,(runpath,run_whole))
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
	finally:
		shutil.rmtree(temp_dir)

if __name__=='__main__':

	#basepath = '/export/silurian/array-01/pu17449/happi_data/'
	basepath='/export/triassic/array-01/pu17449/happi_data2/'
	#models = ['NorESM1-HAPPI','MIROC5','CanAM4','CAM4-2degree']
	#models = ['HadAM3P']
	models = ['CAM5-1-2-025degree']
	experiments = ['All-Hist','Plus15-Future','Plus20-Future']
	#experiments = ['Plus20-Future']
	#varlist = ['pr','tasmin','tasmax','rsds']
	varlist = ['rsds']
	numthreads = 5

	for model in models:
		for experiment in experiments:
			for var in varlist:
				process_data(model,experiment,var,basepath,numthreads)


