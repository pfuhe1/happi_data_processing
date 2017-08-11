# Script to do monthly means then ensemble average of HAPPI data
import os,glob,tempfile,shutil,socket
from threading import Thread
import multiprocessing
from get_runs import get_runs

# Create list into a string (also adding monmean operator)
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

def list_to_string_raindays(l):
	s = ''
	for item in l:
		s += '-expr,"raindays=pr>0.000011574" '+ item +' '
	return s[:-1] # Get rid of final space


# Process data for a single ensemble member/ run
def process_run_raindays(runpath,run_whole):
	run = os.path.basename(runpath)
	print run
	# input files
	run_files=glob.glob(runpath+'/*.nc')

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
		run_cat = run_whole[:-3]+'_cat.nc'
		cdo_cmd = "cdo cat " + list_to_string_raindays(run_files) +" "+ run_cat
		print cdo_cmd
		os.system(cdo_cmd)
		cdo_cmd2 = "cdo ymonmean " + run_cat + " " + run_whole
		os.system(cdo_cmd2)
		os.remove(run_cat)

# Process data for a single ensemble member/ run
def process_run_Rx5day(runpath,run_whole):
	run = os.path.basename(runpath)
	print run
	# input files
	run_files=glob.glob(runpath+'/*.nc')

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
		cdo_cmd = "cdo ymonmax -runmean,5 -cat " +list_to_string(run_files) + " " + run_whole
		print cdo_cmd
		os.system(cdo_cmd)

# Process data for a single ensemble member/ run
def process_run_RXx5day(runpath,run_whole):
	run = os.path.basename(runpath)
	print run
	# input files
	run_files=glob.glob(runpath+'/*.nc')

	if len(run_files)==0:
		raise Exception('error, no files found: '+runpath)
	elif len(run_files)==1:
		# Only one file, calculate RXx5day (annual 5 day max)
		# CDO command
		cdo_cmd = "cdo timmax -runmean,5 " + run_files[0] + " " + run_whole
		print cdo_cmd
		os.system(cdo_cmd)
	else: 
		# calculate RXx5day (annual 5 day max) and concatenate files
		# CDO command
		cdo_cmd = "cdo timmax -runmean,5 -cat " +list_to_string(run_files) + " " + run_whole
		print cdo_cmd
		os.system(cdo_cmd)

# Process all the data for the particular model, experiment and variable
def process_data(model,experiment,var,index,basepath,numthreads):
	data_freq = 'day'

	outpath_runs=os.path.join(basepath,'indices',model,experiment,index)
	if not os.path.exists(outpath_runs):
		os.makedirs(outpath_runs)
	try:		
		print model,experiment,var
		outmean = outdir+model+'.'+index+'.'+experiment+'_monclim_ensmean.nc'
		outstd = outdir+model+'.'+index+'.'+experiment+'_monclim_ensstd.nc'
		if os.path.exists(outmean) and os.path.exists(outstd):
			print 'files already exist, skipping'
			return
		
		# Create pool of processes to process runs in parallel. 
		pool = multiprocessing.Pool(processes=numthreads)

		if index == 'raindays':
			process = process_run_raindays
		elif index == 'Rx5day':
			process = process_run_Rx5day
		elif index == 'RXx5day':
			process = process_run_RXx5day

		# Loop over runs
		run_averages = ''
		runs = get_runs(model,experiment,basepath,data_freq,var)
		for runpath in runs:
			run = os.path.basename(runpath)
			run_whole = os.path.join(outpath_runs,model+'_'+experiment+'_'+index+'_'+run+'.nc')
			pool.apply_async(process,(runpath,run_whole))
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


if __name__=='__main__':

	host=socket.gethostname()
	# CAM5 data is stored and should be processed on triassic
	if host=='triassic.ggy.bris.ac.uk':
		basepath='/export/triassic/array-01/pu17449/happi_data_decade/'
		models = ['CAM5-1-2-025degree']
		# Number of processes to run in parallel to process ensemble members
		numthreads = 5
	# All other data is stored on silurian
	elif host =='silurian.ggy.bris.ac.uk':
		basepath = '/export/silurian/array-01/pu17449/happi_data/'
		models = ['NorESM1-HAPPI','MIROC5','CanAM4','CAM4-2degree','HadAM3P']
		# Number of processes to run in parallel to process ensemble members
		numthreads = 4

	experiments = ['All-Hist','Plus15-Future','Plus20-Future']
	var = 'pr' # Get raindays (number of days where precip > 1mm)
	outdir = '/export/silurian/array-01/pu17449/processed_data_20170627/'
	indices = ['raindays','Rx5day','RXx5day']

	# PROCESS CESM low warming
#	models = ['CESM-CAM5']
	# override defaults
#	basepath = '/export/silurian/array-01/pu17449/CESM_low_warming/decade_data/'
#	experiments = ['historical','1pt5degC','2pt0degC']
#	outdir = '/export/silurian/array-01/pu17449/CESM_low_warming/indices/'

	for model in models:
		for experiment in experiments:
			for index in indices:
				# Call process_data for this model, experiment and variable
				process_data(model,experiment,var,index,basepath,numthreads)
				# Simple Multithreading to call process_data in separate threads
	#			t = Thread(target = process_data, args=(model,experiment,var,basepath))
	#			t.start()

