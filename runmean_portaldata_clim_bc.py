# Script to do monthly means then ensemble average of HAPPI data
import os,glob,tempfile,shutil
import multiprocessing,time
import socket
from get_runs import get_runs

# Create list into a string 
def list_to_string(l):
	s = ''
	for item in l:
		s += item +' '
	return s

# Process data for a single ensemble member/ run
def process_run(runpath,run_whole,operator='mean'):
	run = os.path.basename(runpath)
	print run
	#run_files=glob.glob(runpath+'/*.nc')
	# runpath is just one file:
	run_files=[runpath]
	

	if len(run_files)==0:
		raise Exception('error, no files found: '+fnames)
	elif len(run_files)==1:
		# Only one file
		# CDO command
		cdo_cmd = 'cdo tim'+operator+' ' + run_files[0] + ' ' + run_whole
		print cdo_cmd
		os.system(cdo_cmd)
	else: 
		# concatenate files
		# CDO command
		cdo_cmd = "cdo tim"+operator+" -cat '" + list_to_string(run_files) + "' "+ run_whole
		print cdo_cmd
		os.system(cdo_cmd)

# Process data for a single ensemble member/ run
def process_run_raindays(runpath,run_whole,unit_conv):
	
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
		cdo_cmd = "cdo timmean -expr,'raindays=pr>"+thresh+"' " + run_files[0] + " " + run_whole
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


# Process all the data for the particular model, experiment and variable
def process_data(model,experiment,var,basepath,numthreads,data_freq,period):
	thresh = str(1/86400.) # Threshold (1mm/day)
	operators = ['std',"mean -expr,'raindays=pr>"+thresh+"'"]#,'min','max','pctl,95','pctl,5']
	opnames = ['std','raindays']
	op_ensmean=['']*len(operators)
	op_ensstd=['']*len(operators)
	run_averages = ''
	run_averages_op = ['']*len(operators)
	out_ensmean = os.path.join(outdir,'run_ensmean/')
	if not os.path.exists(out_ensmean):
		os.makedirs(out_ensmean)
	try:		
		print model,experiment,var
		clim_ensmean = out_ensmean+model+'.'+var+'.'+experiment+'_'+period+'_runmean_ensmean.nc'
		clim_ensstd = out_ensmean+model+'.'+var+'.'+experiment+'_'+period+'_runmean_ensstd.nc'
		#if data_freq=='day':
		for i,op in enumerate(operators):
			opname = opnames[i]
			op_ensmean[i] = out_ensmean+model+'.'+var+'.'+experiment+'_'+period+'_run'+opname+'_ensmean.nc'
			op_ensstd[i] = out_ensmean+model+'.'+var+'.'+experiment+'_'+period+'_run'+opname+'_ensstd.nc'

	#	if os.path.exists(clim_ensmean) and os.path.exists(clim_ensstd):
	#		print 'files already exist, skipping'
	#		return

		# Create pool of processes to process runs in parallel. 
		pool = multiprocessing.Pool(processes=numthreads)

		# Get list of runs
		#runs = get_runs(model,experiment,basepath,data_freq,var)
		runpattern = os.path.join(basepath,model,'EWEMBI',var+'_'+data_freq+'_'+model+'_'+experiment+'_*_'+period+'.nc')
		print runpattern
		runs = glob.glob(runpattern)

		outpath_runs=os.path.join(outdir,'run_data',model,experiment,var)
		if not os.path.exists(outpath_runs):
			os.makedirs(outpath_runs)

		# Loop over runs
		for runpath in runs:
			run = os.path.basename(runpath)
			# Calculate ymonmean
			run_whole = os.path.join(outpath_runs,run[:-3]+'_runmean.nc')
			# Add the process to the pool
			if not os.path.exists(run_whole):
				pool.apply_async(process_run,(runpath,run_whole))
				#process_run(runpath,run_whole)
			# Add this run to list
			run_averages += run_whole +' '

		# close the pool and make sure the processing has finished
		pool.close()
		pool.join()

		# Ensemble mean
		if not os.path.exists(clim_ensmean):
			cdo_cmd = 'cdo ensmean ' + run_averages + clim_ensmean
			print cdo_cmd
			os.system(cdo_cmd)

		# Ensemble stdev
		if not os.path.exists(clim_ensstd):
			cdo_cmd = 'cdo ensstd ' + run_averages + clim_ensstd
			print cdo_cmd
			os.system(cdo_cmd)

		# Apply other operators 
		for i,op in enumerate(operators):
			#opname = op.replace(',','') 
			opname = opnames[i]

			# Create pool of processes to process runs in parallel. 
			pool = multiprocessing.Pool(processes=numthreads)
	
			# Loop over runs
			for runpath in runs:
				run = os.path.basename(runpath)
				#runname_start= os.path.join(outpath_runs,model+'_'+experiment+'_'+var+'_'+run+'_run')
				runname_start= os.path.join(outpath_runs,run[:-3]+'_run')
				# percentile calculations require ymonmin and ymonmax as input
				if op[:4]=='pctl':
					run_whole_op = runname_start+'min.nc '+ runname_start+'max.nc '+runname_start+opname+'.nc'
				else:
					run_whole_op = runname_start+opname+'.nc'

				if not os.path.exists(run_whole_op.split(' ')[-1]):
					pool.apply_async(process_run,(runpath,run_whole_op),{'operator':op})
				run_averages_op[i] += run_whole_op +' '

			# close the pool and make sure the processing has finished
			pool.close()
			pool.join()				

			# Ensemble mean
			if not os.path.exists(op_ensmean[i]):
				cdo_cmd = 'cdo ensmean ' + run_averages_op[i] + op_ensmean[i]
				print cdo_cmd
				os.system(cdo_cmd)

			if not os.path.exists(op_ensstd[i]):
				# Ensemble stdev
				cdo_cmd = 'cdo ensstd ' + run_averages_op[i] + op_ensstd[i]
				print cdo_cmd
				os.system(cdo_cmd)

	except Exception,e:
		print 'Error in script: '
		print e
		raise

if __name__=='__main__':

	host=socket.gethostname()
	# CAM5 data is stored and should be processed on triassic
	if host =='anthropocene.ggy.bris.ac.uk':
		basepath = '/export/anthropocene/array-01/pu17449/happi_data_corrected/'
		outdir = '/export/anthropocene/array-01/pu17449/corrected_data_processed/'
		models = ['NorESM1-HAPPI','CAM4-2degree',]#'MIROC5','CanAM4','HadAM3P']
		# Number of processes to run in parallel to process ensemble members
		numthreads = 5

	experiments = ['All-Hist']#,'All-Hist','Plus15-Future','Plus20-Future']
	varlist = ['pr']#,'tas']
	#data_freq = {'pr':'mon','tas':'mon'}
	data_freq = 'day'
	periods = ['19790101-20131231','20060101-20151231','21060101-21151231','21060101-21151231']

	for model in models:
		# Exception to the data_freq list above is HadAM3P which has daily 'rsds' and 'tas'
#		if model == 'HadAM3P':
#			data_freq['pr']='day'
#			data_freq['tas']='day'
		for i,experiment in enumerate(experiments):
			period = periods[i]
			for var in varlist:
				process_data(model,experiment,var,basepath,numthreads,data_freq,period)


