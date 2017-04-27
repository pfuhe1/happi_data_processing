# Script to do monthly means then ensemble average of HAPPI data
import os,glob,tempfile,shutil
from threading import Thread


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

# Process data for a single ensemble member/ run
def process_run(runpath,model,experiment,var,temp_dir):
	run = os.path.basename(runpath)
	print run
	# input files
	run_files=glob.glob(runpath+'/*.nc')
	# output file
	run_whole = os.path.join(temp_dir,model+'_'+experiment+'_raindays_'+run+'.nc')
	if len(run_files)==0:
		raise Exception('error, no files found: '+runpath)
	elif len(run_files)==1:
		# Only one file, calculate raindays
		# CDO command
		cdo_cmd = "cdo ymonmean -expr,'raindays=pr>0.000011574' " + run_files[0] + " " + run_whole
		print cdo_cmd
		os.system(cdo_cmd)
	else: 
		# calculate raindays and concatenate files (have to do in two parts)
		# CDO command
		run_cat = os.path.join(temp_dir,model+'_'+experiment+'_raindays_'+run+'_cat.nc')
		cdo_cmd = "cdo cat " + list_to_string_raindays(run_files) +" "+ run_cat
		print cdo_cmd
		os.system(cdo_cmd)
		cdo_cmd2 = "cdo ymonmean " + run_cat + " " + run_whole
		os.system(cdo_cmd2)
		os.remove(run_cat)

# Process all the data for the particular model, experiment and variable
def process_data(model,experiment,var,basepath,numthreads):
	temp_dir = tempfile.mkdtemp(dir='/export/silurian/array-01/pu17449/')
	try:		
		print model,experiment,var
		outmean = '/export/silurian/array-01/pu17449/processed_data_clim/'+model+'.raindays.'+experiment+'_monclim_ensmean.nc'
		outstd = '/export/silurian/array-01/pu17449/processed_data_clim/'+model+'.raindays.'+experiment+'_monclim_ensstd.nc'
		if os.path.exists(outmean) and os.path.exists(outstd):
			print 'files already exist, skipping'
			return
		
		# Create pool of processes to process runs in parallel. 
		pool = multiprocessing.Pool(processes=numthreads)

		# Loop over runs
		run_averages = ''
		runs = get_runs(model,experiment,var,basepath)
		for runpath in runs:
			pool.apply_async(process_run,(runpath,model,experiment,temp_dir))
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

	basepath = '/export/silurian/array-01/pu17449/happi_data/'
	models = ['NorESM1-HAPPI','MIROC5','CanAM4','CAM4-2degree']
	experiments = ['All-Hist','Plus15-Future','Plus20-Future']
	var = 'pr' # Get raindays from precip > 1mm
	numthreads = 6

	for model in models:
		for experiment in experiments:
			# Call process_data for this model, experiment and variable
			process_data(model,experiment,var,basepath,numthreads)
			# Simple Multithreading to call process_data in separate threads
#			t = Thread(target = process_data, args=(model,experiment,var,basepath))
#			t.start()

