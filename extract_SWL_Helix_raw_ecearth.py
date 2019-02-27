# Script to do monthly means then ensemble average of HAPPI data
import os,glob,tempfile,shutil
from helix_swls import swl_years


# Process all the data for the particular model, experiment and variable
def process_data(model,experiment,var,basepath,outpath,data_freq):
	print model,experiment,var
	if data_freq != 'day':
		raise Exception('Script can only process daily data, needs work to convert to monthly')

	# Loop over runs
	inruns = os.path.join(basepath,var,'*')
	runs = glob.glob(inruns)

	for runpath in runs:
		try:
			ens = os.path.basename(runpath)
			if ens == 'r0i1p1': # skip r0 (forced by ERA-Interim)
				continue

	                outpath_runs=os.path.join(outpath,'SWL_data',model,experiment,'est1/v1-0/day/atmos',var,ens)
	                if not os.path.exists(outpath_runs):
        	                os.makedirs(outpath_runs)
			else:
				print 'path already exists, not making directory',outpath_runs
	
			if experiment == 'All-Hist':
				swl_year = 2010
			else:
				swl_year = swl_years[experiment][ens]
			
			for year in range(swl_year-5,swl_year+6):
				print year
				fname = var+'_'+data_freq+'_'+model+'_rcp85_'+ens+'_'+str(year)+'0101-'+str(year)+'1231.nc'
				infile = os.path.join(runpath,fname)
				outfile = os.path.join(outpath_runs,fname)
				if not os.path.exists(outfile):
					print outfile
					os.symlink(infile,outfile)
				else:
					print 'file exists',outfile
				

		except Exception,e:
			print 'Error in script: '
			print e

if __name__=='__main__':

	#basepath = '/group_workspaces/jasmin2/mohc_shared/users/sarahshannon/helix/'
	#basepath = '/group_workspaces/jasmin2/mohc_shared_OLD/users/dmhg/helix/MO/HadGEM3/rcp85/day/',
	basepath = '/group_workspaces/jasmin2/mohc_shared_OLD/users/dmhg/helix/SMHI/EC-EARTH3-HR/rcp85/day/atmos/'
	outpath = '/work/scratch/pfu599/helix_data'
	experiments = ['All-Hist','Plus15-Future','Plus20-Future']
	models = ['EC-EARTH3-HR']

	data_freq = 'day'
	varlist = ['tas','pr']

	for model in models:
		for experiment in experiments:
			for var in varlist:
				process_data(model,experiment,var,basepath,outpath,data_freq)


