# Script to do monthly means then ensemble average of HAPPI data
import os,glob,tempfile,shutil
from helix_swls import swl_years


# Process all the data for the particular model, experiment and variable
def process_data(model,experiment,var,basepath,outpath,data_freq):
	print(model,experiment,var)
	if data_freq != 'day':
		raise Exception('Script can only process daily data, needs work to convert to monthly')

	# Loop over runs
	inruns = os.path.join(basepath,model,var,'*')
	runs = glob.glob(inruns)

	for runpath in runs:
		try:
			ens = os.path.basename(runpath)
			if len(ens)!=6:
				continue # skip 'original' runs e.g. r1i1p1_original
			else:
				outpath_runs=os.path.join(outpath,model,experiment,'est1/v1-0/day/atmos',var,ens)
				if not os.path.exists(outpath_runs):
					os.makedirs(outpath_runs)
				else:
					print('path already exists, not creating directory',outpath_runs)
	
				if experiment == 'historical':
					swl_year = 2010
				else:
					swl_year = swl_years[experiment][ens]
			
				for year in range(swl_year-10,swl_year+11):
					print(year)
					fname = var+'_'+model+'_'+ens+'_'+str(year)+'.nc'
					infile = os.path.join(runpath,fname)
					outfile = os.path.join(outpath_runs,fname)
					if not os.path.exists(outfile):
						print(outfile)
						os.symlink(infile,outfile)
					else:
						print('file exists',outfile)

		except Exception as e:
			print('Error in script: ')
			print(e)

if __name__=='__main__':

	#basepath = '/group_workspaces/jasmin2/mohc_shared_OLD/users/sarahshannon/helix/'
	basepath = '/gws/nopw/j04/mohc_shared/users/sarahshannon/helix/'
	#outpath = '/work/scratch/pfu599/helix_data'
	outpath = '/gws/nopw/j04/bris_climdyn/pfu599/timeslice_data/'

	experiments = ['historical','slice15','slice20']
	models = ['ec-earth3-hr','hadgem3']

	data_freq = 'day'
	varlist = ['tas','pr']

	for model in models:
		for experiment in experiments:
			for var in varlist:
				process_data(model,experiment,var,basepath,outpath,data_freq)


