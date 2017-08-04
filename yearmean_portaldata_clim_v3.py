# Script to do monthly means then ensemble average of HAPPI data
import os,glob,tempfile,shutil
import multiprocessing,time
import socket
from get_runs import get_runs


timesel = '-seltimestep'
for t in range(3,120):
	timesel=timesel+','+str(t)
timesel = timesel+' '

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
		cdo_cmd = 'cdo ymon'+operator+' -yearmean ' + run_files[0] + ' ' + run_whole
		print cdo_cmd
		os.system(cdo_cmd)
	else: 
		# concatenate files
		# CDO command
		cdo_cmd = "cdo ymon"+operator+" -yearmean -cat '" + list_to_string(run_files) + "' "+ run_whole
		print cdo_cmd
		os.system(cdo_cmd)


# Process all the data for the particular model, experiment and variable
def process_data(model,experiment,var,basepath,numthreads,data_freq):
	operators = ['std']#,'min','max','pctl,95','pctl,5']
	op_ensmean=['']*len(operators)
	op_ensstd=['']*len(operators)
	run_averages = ''
	run_averages_op = ['']*len(operators)
	try:		
		print model,experiment,var
		clim_ensmean = outdir+model+'.'+var+'.'+experiment+'_yearclim_ensmean.nc'
		clim_ensstd = outdir+model+'.'+var+'.'+experiment+'_yearclim_ensstd.nc'
		#if data_freq=='day':
		for i,op in enumerate(operators):
			opname = op.replace(',','')
			op_ensmean[i] = outdir+model+'.'+var+'.'+experiment+'_year'+opname+'_ensmean.nc'
			op_ensstd[i] = outdir+model+'.'+var+'.'+experiment+'_year'+opname+'_ensstd.nc'

	#	if os.path.exists(clim_ensmean) and os.path.exists(clim_ensstd):
	#		print 'files already exist, skipping'
	#		return

		# Create pool of processes to process runs in parallel. 
		pool = multiprocessing.Pool(processes=numthreads)

		# Get list of runs
		runs = get_runs(model,experiment,basepath,data_freq,var)

		outpath_runs=os.path.join(basepath,'seas_data',model,experiment,var)
		if not os.path.exists(outpath_runs):
			os.makedirs(outpath_runs)

		# Loop over runs
		for runpath in runs:
			run = os.path.basename(runpath)
			# Calculate ymonmean
			run_whole = os.path.join(outpath_runs,model+'_'+experiment+'_'+var+'_'+run+'_year.nc')
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

		# Apply other operators 
		for i,op in enumerate(operators):
			opname = op.replace(',','') 

			# Create pool of processes to process runs in parallel. 
			pool = multiprocessing.Pool(processes=numthreads)
	
			# Loop over runs
			for runpath in runs:
				run = os.path.basename(runpath)
				runname_start= os.path.join(outpath_runs,model+'_'+experiment+'_'+var+'_'+run+'_year')
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

	outdir = '/export/silurian/array-01/pu17449/seas_data_20170731/'

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
		numthreads = 1

	experiments = ['All-Hist','Plus15-Future','Plus20-Future']
	varlist = ['pr','tas']
	data_freq = {'pr':'mon','tas':'mon'}

	# PROCESS CESM low warming
	#models = ['CESM-CAM5']
	# override defaults
	#basepath = '/export/silurian/array-01/pu17449/CESM_low_warming/decade_data/'
	#experiments = ['historical','1pt5degC','2pt0degC']
#	outdir = '/export/silurian/array-01/pu17449/CESM_low_warming/seas_data_CESM/'

	for model in models:
		# Exception to the data_freq list above is HadAM3P which has daily 'rsds' and 'tas'
#		if model == 'HadAM3P':
#			data_freq['pr']='day'
#			data_freq['tas']='day'
		for experiment in experiments:
			for var in varlist:
				process_data(model,experiment,var,basepath,numthreads,data_freq[var])


