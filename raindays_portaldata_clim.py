# Script to do monthly means then ensemble average of HAPPI data
import os,glob,tempfile,shutil
from threading import Thread
import multiprocessing
from get_runs import get_runs


# Create list into a string (also adding monmean operator)
def list_to_string_monmean(l):
	s = ''
	for item in l:
		s += '-monmean '+ item +' '
#		s += item +' '
	return s


# Process data for a single ensemble member/ run
def process_run(runpath,run_whole):
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

# Process all the data for the particular model, experiment and variable
def process_data(model,experiment,var,basepath,numthreads):
	temp_dir = tempfile.mkdtemp(dir=basepath+'/../')
	data_freq = 'day'
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
		runs = get_runs(model,experiment,basepath,data_freq,var)
		for runpath in runs:
			run = os.path.basename(runpath)
			run_whole = os.path.join(temp_dir,model+'_'+experiment+'_'+var+'_'+run+'.nc')
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

	basepath = '/export/silurian/array-01/pu17449/happi_data/'
	#basepath='/export/triassic/array-01/pu17449/happi_data2/'
	#models = ['NorESM1-HAPPI','MIROC5','CanAM4','CAM4-2degree']
	#models = ['CAM5-1-2-025degree']
	models = ['CAM4-2degree']
	experiments = ['All-Hist','Plus15-Future','Plus20-Future']
	var = 'pr' # Get raindays from precip > 1mm
	numthreads = 12

	for model in models:
		for experiment in experiments:
			# Call process_data for this model, experiment and variable
			process_data(model,experiment,var,basepath,numthreads)
			# Simple Multithreading to call process_data in separate threads
#			t = Thread(target = process_data, args=(model,experiment,var,basepath))
#			t.start()

