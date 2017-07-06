# Script to do monthly means then ensemble average of HAPPI data
import os,glob,tempfile,shutil
import multiprocessing,time


# Create list into a string 
def list_to_string(l):
	s = ''
	for item in l:
		s += item +' '
	return s


# Process data for a single ensemble member/ run (add two fields together)
def process_run_add(experiment,runpath1,runpath2,var,var_rename,run_whole):
	run = os.path.basename(runpath1)
	print run
	if experiment=='historical':
		#seldate = '-seldate,1995-01-01,2005-12-31 '
		# Hack to shift date to centre of month rather than the start of the following month
		seldate = '-seldate,1995-01-01,2005-12-31 -shifttime,-15 '
	else:
		#seldate = '-seldate,2091-01-01,2100-12-31 
		# Hack to shift date to centre of month rather than the start of the following month
		seldate = '-seldate,2091-01-01,2100-12-31 -shifttime,-15 '


	# CDO command
	cdo_cmd = 'cdo -add '+seldate + runpath1 + ' ' + seldate + runpath2 + ' ' + run_whole[:-3]+'_tmp.nc'
	print cdo_cmd
	os.system(cdo_cmd)

	# rename variable
	nco_cmd = 'ncrename -v '+var+','+var_rename[var] +' '+run_whole[:-3]+'_tmp.nc'
	os.system(nco_cmd)

	# convert to netcdf3 (from netcdf4)
	nco_cmd = 'ncks -3 '+run_whole[:-3]+'_tmp.nc '+run_whole
	os.system(nco_cmd)

	os.remove(run_whole[:-3]+'_tmp.nc')

# Process data for a single ensemble member/ run
def process_run(experiment,runpath1,var,var_rename,run_whole):
	run = os.path.basename(runpath1)
	print run
	if experiment=='historical':
		#seldate = '-seldate,1995-01-01,2005-12-31 '
		# Hack to shift date to centre of month rather than the start of the following month
		seldate = '-seldate,1995-01-01,2005-12-31 -shifttime,-15 '
	else:
		#seldate = '-seldate,2091-01-01,2100-12-31 
		# Hack to shift date to centre of month rather than the start of the following month
		seldate = '-seldate,2091-01-01,2100-12-31 -shifttime,-15 '

	# CDO command
	cdo_cmd = 'cdo '+seldate + runpath1 + ' ' + run_whole[:-3]+'_tmp.nc'
	print cdo_cmd
	os.system(cdo_cmd)

	# rename variable
	nco_cmd = 'ncrename -v '+var+','+var_rename[var] +' '+run_whole[:-3]+'_tmp.nc'
	os.system(nco_cmd)

	# convert to netcdf3 (from netcdf4)
	nco_cmd = 'ncks -3 '+run_whole[:-3]+'_tmp.nc '+run_whole
	os.system(nco_cmd)	

	os.remove(run_whole[:-3]+'_tmp.nc')

# Process all the data for the particular model, experiment and variable
def process_data(experiment,var,basepath,numthreads,data_freq):
	var_rename={'TREFHT':'tas','PRECL':'pr'}
	try:		
		print experiment,var

		# Create pool of processes to process runs in parallel. 
		pool = multiprocessing.Pool(processes=numthreads)

		# Loop over runs
		run_averages = ''
		runstring = basepath+experiment+'/mon/'+var+'/*.'+var+'.*.nc'
		print runstring
		runs = glob.glob(runstring)
		print runs

		outpath_runs=os.path.join(basepath,'decade_data',experiment)
		if not os.path.exists(outpath_runs):
			os.mkdir(outpath_runs)		
	
		for runpath in runs:
			run = os.path.basename(runpath).split('.cam.')[0][-3:]
			run_whole = os.path.join(outpath_runs,data_freq,var_rename[var],'run'+run,'CESM-CAM5_'+var_rename[var]+'_'+experiment+'_'+run+'_decade.nc')
			if not os.path.exists(run_whole):
				if not os.path.exists(os.path.dirname(run_whole)):
					os.makedirs(os.path.dirname(run_whole))

				# Add the process to the pool
				if var == 'PRECL':
					runpath2 = runpath.replace('PRECL','PRECC')
					pool.apply_async(process_run_add,(experiment,runpath,runpath2,var,var_rename,run_whole))
				else:
					pool.apply_async(process_run,(experiment,runpath,var,var_rename,run_whole))

		# close the pool and make sure the processing has finished
		pool.close()
		pool.join()

	

	except Exception,e:
		print 'Error in script: '
		print e
		raise

if __name__=='__main__':

	basepath = '/export/silurian/array-01/pu17449/CESM_low_warming/'
	experiments = ['historical','1pt5degC','2pt0degC','1pt5degC_OS']
	varlist = ['PRECL','TREFHT']
	numthreads = 11
	data_freq = 'mon'

	for experiment in experiments:
		for var in varlist:
			process_data(experiment,var,basepath,numthreads,data_freq)


