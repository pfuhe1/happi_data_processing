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
	operators = ['std']#,'min','max','pctl,95','pctl,5']
	op_ensmean=['']*len(operators)
	op_ensstd=['']*len(operators)
	run_averages = ''
	run_averages_op = ['']*len(operators)
	out_ensmean = os.path.join(outdir,'monclim_ensdata')
	if not os.path.exists(out_ensmean):
		os.makedirs(out_ensmean)
	try:		
		print model,experiment,var
		clim_ensmean = out_ensmean+'/'+model+'.'+var+'.'+experiment+'_monclim_ensmean.nc'
		clim_ensstd = out_ensmean+'/'+model+'.'+var+'.'+experiment+'_monclim_ensstd.nc'
		if data_freq=='day':
			for i,op in enumerate(operators):
				opname = op.replace(',','')
				op_ensmean[i] = out_ensmean+'/'+model+'.'+var+'.'+experiment+'_mon'+opname+'_ensmean.nc'
				op_ensstd[i] = out_ensmean+'/'+model+'.'+var+'.'+experiment+'_mon'+opname+'_ensstd.nc'

	#	if os.path.exists(clim_ensmean) and os.path.exists(clim_ensstd):
	#		print 'files already exist, skipping'
	#		return

		# Create pool of processes to process runs in parallel. 
		pool = multiprocessing.Pool(processes=numthreads)

		# Get list of runs
		runs = get_runs(model,experiment,basepath,data_freq,var)

		outpath_runs=os.path.join(outdir,'monclim_data',model,experiment,var)
		if not os.path.exists(outpath_runs):
			os.makedirs(outpath_runs)

		# Loop over runs
		for runpath in runs:
			run = os.path.basename(runpath)
			# Calculate ymonmean
			run_whole = os.path.join(outpath_runs,model+'_'+experiment+'_'+var+'_'+run+'_clim.nc')
			# Add the process to the pool
			if not os.path.exists(run_whole):
				pool.apply_async(process_run,(runpath,run_whole))
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

		# Apply other operators on daily data
		if data_freq == 'day':
			for i,op in enumerate(operators):
				opname = op.replace(',','') 

				# Create pool of processes to process runs in parallel. 
				pool = multiprocessing.Pool(processes=numthreads)
		
				# Loop over runs
				for runpath in runs:
					run = os.path.basename(runpath)
					runname_start=run_whole = os.path.join(outpath_runs,model+'_'+experiment+'_'+var+'_'+run+'_')
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
		numthreads = 16
		outdir = '/export/silurian/array-01/pu17449/happi_processed/'
	elif host == 'happi.ggy.bris.ac.uk':
		basepath = '/data/scratch/happi_data/'
		models = ['NorESM1-HAPPI','MIROC5','CanAM4','CAM4-2degree','HadAM3P','CESM-CAM5']
		numthreads=20
		outdir = '/data/scratch/pu17449/happi_processed/'
	elif host =='anthropocene.ggy.bris.ac.uk':
		basepath = '/export/anthropocene/array-01/pu17449/happi_data/'
		#models = ['NorESM1-HAPPI','MIROC5','CanAM4','CAM4-2degree','HadAM3P']
		models = ['NorESM1-HAPPI','MIROC5','CanAM4','CAM4-2degree','HadAM3P','CESM-CAM5','ECHAM6-3-LR']
		# Number of processes to run in parallel to process ensemble members
		numthreads = 4
		outdir = '/export/anthropocene/array-01/pu17449/happi_processed'

	experiments = ['All-Hist','Plus15-Future','Plus20-Future']
	varlist = ['pr',]#'tasmin','tasmax','rsds','tas']
	data_freq = {'pr':'day','tasmin':'day','tasmax':'day','tas':'mon','rsds':'mon'}


	for model in models:
		# Exception to the data_freq list above is HadAM3P which has daily 'rsds' and 'tas'
		#if model == 'HadAM3P':
			#data_freq['rsds']='day'
			#data_freq['tas']='day'
		for experiment in experiments:
			for var in varlist:
				process_data(model,experiment,var,basepath,numthreads,data_freq[var])


