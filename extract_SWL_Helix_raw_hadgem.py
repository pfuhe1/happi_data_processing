# Script to do monthly means then ensemble average of HAPPI data
import os,glob,tempfile,shutil
from helix_swls import swl_years
import subprocess,multiprocessing


# Process all the data for the particular model, experiment and variable
def process_data(model,experiment,var,basepath,outpath,data_freq,numthreads=2):
	print(model,experiment,var)
	if data_freq != 'day':
		raise Exception('Script can only process daily data, needs work to convert to monthly')

	# Loop over runs
	inruns = os.path.join(basepath,'*')
	runs = glob.glob(inruns)

	pool = multiprocessing.Pool(processes=numthreads)

	for runpath in runs:
		try:

			fpattern = os.path.join(runpath,var+'_'+data_freq+'_'+model+'*.nc')
			print(fpattern)
			fin = glob.glob(fpattern)[0]
			ens = os.path.basename(runpath)
			ens = ens+'i1p1' # convert e.g. 'r1' to r1i1p1
			if ens == 'r0i1p1': # skip r0 (forced by ERA-Interim)
				continue
			
			outpath_runs=os.path.join(outpath,model,experiment,'est1/v1-0/day/atmos',var,ens)
			if not os.path.exists(outpath_runs):
				os.makedirs(outpath_runs)
			else:
				print('path already exists, not creating directory',outpath_runs)

			# Determine specific warming level years for experiment
			if experiment == 'historical':
				swl_year = 2010
			else:
				swl_year = swl_years[experiment][ens]
			iyear = swl_year-10
			eyear = swl_year+10

			# Extract data just from the SWL period
			seldate_str = 'seldate,'+str(iyear)+'-01-01,'+str(eyear)+'-12-31'
			fdate_str = str(iyear)+'0101-'+str(eyear)+'1231'
			fout = var+'_'+model+'_'+ens+'_'+fdate_str+'.nc'
			infile = os.path.join(runpath,fin)
			outfile = os.path.join(outpath_runs,fout)
			if not os.path.exists(outfile):				
				cdo_cmd = ['cdo',seldate_str,infile,outfile]
				print(cdo_cmd)
				pool.apply_async(subprocess.call,(cdo_cmd,))
				#subprocess.call(cdo_cmd)
			else:
				print('file exists',outfile)
			
		except Exception as e:
			print('Error in script: ')
			print(e)

	pool.close()
	pool.join()

if __name__=='__main__':

	#basepath = '/group_workspaces/jasmin2/mohc_shared/users/sarahshannon/helix/'
	#basepath = '/group_workspaces/jasmin2/mohc_shared_OLD/users/dmhg/helix/MO/HadGEM3/rcp85/day/'
	basepath = '/gws/nopw/j04/mohc_shared/users/dmhg/helix/MO/HadGEM3/rcp85/day/'
	#basepath = '/group_workspaces/jasmin2/mohc_shared_OLD/users/dmhg/helix/SMHI/EC-EARTH3-HR/rcp85/day/atmos/'
	outpath = '/gws/nopw/j04/bris_climdyn/pfu599/timeslice_data/'
	experiments = ['historical','slice15','slice20']
	models = ['HadGEM3']

	data_freq = 'day'
	varlist = ['tas','pr']

	for model in models:
		for experiment in experiments:
			for var in varlist:
				process_data(model,experiment,var,basepath,outpath,data_freq)


