# Script to do monthly means then ensemble average of HAPPI data
import os,glob,tempfile,shutil
from helix_swls import swl_years


mip_table = {'day':'day','mon':'Amon'}

def datestr(year,data_freq):
	if data_freq == 'day':
		return str(year)+'0101-'+str(year)+'1231'
	elif data_freq == 'mon':
		return str(year)+'01-'+str(year)+'12'
	else:
		raise Exception('Error, can only handle "day" or "mon" data')

# Process all the data for the particular model, experiment and variable
def process_data(model,experiment,var,basepath,outpath,data_freq,domain = 'atmos'):
	print(model,experiment,var)

	# Loop over runs
	inruns = os.path.join(basepath,data_freq,domain,var,'*')
	runs = glob.glob(inruns)

	for runpath in runs:
		try:
			ens = os.path.basename(runpath)
			if ens == 'r0i1p1': # skip r0 (forced by ERA-Interim)
				continue

			outpath_runs=os.path.join(outpath,model,experiment,'est1/v1-0/'+data_freq+'/atmos',var,ens)
			if not os.path.exists(outpath_runs):
				os.makedirs(outpath_runs)
			else:
				print('path already exists, not making directory',outpath_runs)
	
			if experiment == 'historical':
				swl_year = 2010
			else:
				swl_year = swl_years[experiment][ens]
			
			for year in range(swl_year-10,swl_year+11):
				print(year)
				dates = datestr(year,data_freq)
				fname = var+'_'+mip_table[data_freq]+'_'+model+'_rcp85_'+ens+'_'+dates+'.nc'
				infile = os.path.join(runpath,fname)
				outfile = os.path.join(outpath_runs,fname)
				if not os.path.exists(infile):
					raise Exception('Error, infile doesnt exist: '+infile)
				if not os.path.exists(outfile):
					print(outfile)
					os.symlink(infile,outfile)
				else:
					print('file exists',outfile)
				

		except Exception as e:
			print('Error in script: ')
			print(e)

if __name__=='__main__':

	#basepath = '/group_workspaces/jasmin2/mohc_shared_OLD/users/dmhg/helix/SMHI/EC-EARTH3-HR/rcp85/'
	basepath = '/gws/nopw/j04/mohc_shared/users/dmhg/helix/SMHI/EC-EARTH3-HR/rcp85/'
	outpath = '/gws/nopw/j04/bris_climdyn/pfu599/timeslice_data/'
	experiments = ['historical','slice15','slice20']
	models = ['EC-EARTH3-HR']

	data_freqs = ['day','mon']
	varlist = ['tas','pr']

	for model in models:
		for experiment in experiments:
			for var in varlist:
				for data_freq in data_freqs:
					process_data(model,experiment,var,basepath,outpath,data_freq)


