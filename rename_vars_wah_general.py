# Script to do monthly means then ensemble average of HAPPI data
import os,glob,tempfile,shutil
import multiprocessing,time
from netCDF4 import Dataset
import socket
import subprocess
import time

#########################################################################
# Create list into a string 
#
def list_to_string(l):
	s = ''
	for item in l:
		s += item +' '
	return s

#########################################################################
# Function to rename variables and dimensions of a netcdf file
# Assumes dimensions are in the format: [time, lev, lat, lon]
# outputs lev as either 'height' or 'plev'
#
def rename_vars(fname,varin,varout):

	# Work out input dimension names:
	with Dataset(fname,'r') as f_nc:
		dimlist= f_nc.variables[varin].dimensions
		if dimlist[1][0]=='z':
			height_levs = f_nc.dimensions[dimlist[1]].size
			if height_levs==1:
				levname='height'
			else:
				levname = 'plev'
		else:
			raise Exception('Error, not correctly determining lev dimension: '+str(dimlist))

	# rename variable and spatial dimensions
	nco_cmd = ['ncrename',
	'-v', varin+','+varout,
	'-d', dimlist[3]+',lon',
	'-v', dimlist[3]+',lon',
	'-d', dimlist[2]+',lat',
	'-v', dimlist[2]+',lat',
	'-d', dimlist[1]+','+levname,
	'-v', dimlist[1]+','+levname, 
	fname]
	print nco_cmd
	subprocess.call(nco_cmd)

	# Rename time dimension
	nco_cmd = ['ncrename' ,'-d',dimlist[0]+',time', '-v',dimlist[0]+',time', fname ]
	print nco_cmd
	subprocess.call(nco_cmd)

#########################################################################
# Process data for a single ensemble member/ run (add two fields together)
#
def process_run_add(runpath1,runpath2,varin,varout,run_whole):
	run = os.path.basename(runpath1)
	print run

	# CDO command
	cdo_cmd = ['cdo','-add', runpath1, runpath2, run_whole ]
	print cdo_cmd
	subprocess.call(cdo_cmd)

	# Rename variables and dimensions
	rename_vars(run_whole,varin,varout)

#########################################################################
# Process data for a single ensemble member/ run (mean of two fields)
#
def process_run_mean(runpath1,runpath2,varin,varout,run_whole):
	run = os.path.basename(runpath1)
	print run

	# CDO command
	cdo_cmd = ['cdo', '-divc,2', '-add', runpath1, runpath2, run_whole ]
	print cdo_cmd
	subprocess.call(cdo_cmd)

	# Rename variables and dimensions
	rename_vars(run_whole,varin,varout)

#########################################################################
# Process data for a single ensemble member/ run
#
def process_run(runpath1,varin,varout,run_whole,data_freq):
	run = os.path.basename(runpath1)
	print run

	# no need for cdo command, just copy file
	shutil.copy2(runpath1,run_whole)

	# Rename variables and dimensions
	rename_vars(run_whole,varin,varout)

#########################################################################
# Process all the data for the particular model, experiment and variable
#
def process_data(exp_in,exp_out,varin,varout,basepath,numthreads,domain,est,ver):

	# Get data_freq from var name 
	# Assumes varin is in format e.g. itemXXXXX_monthly_mean
	data_freq_map = {'daily':'day','monthly':'mon'}
	data_freq = data_freq_map[varin.split('_')[1]]

	domain_out = 'atmos'
	# Exception for land variables
	if varout in ['mrro','mrros']:
		domain_out = 'land'
	
	try:		
		print exp_in,varin
		print exp_out,varout

		# Create pool of processes to process runs in parallel. 
		pool = multiprocessing.Pool(processes=numthreads)

		# Get list of runs
		runstring = basepath+'/'+exp_in+'/'+domain+'/'+varin+'/'+varin+'*.nc'
		print runstring
		runs = glob.glob(runstring)
		# Allow time for partially created files to finish being created/copied
		time.sleep(1)
		outpath_runs = os.path.join(baseout,exp_out,est,ver,data_freq,domain_out,varout)

		# loop over runs to process
		#
		for runpath in runs:
			tmp = os.path.basename(runpath)
			a,b,c,run,tim1,tim2 = tmp.split('_')
			datestr = tim1[:4]+tim1[5:7]+ '-'+ tim2[:4]+tim2[5:]
			run_whole = os.path.join(outpath_runs,'run'+run,varout+'_'+'A'+data_freq+'_HadAM3P_'+exp_out+'_'+est+'_'+ver+'_run'+run+'_'+datestr)
			if not os.path.exists(run_whole):
				if not os.path.exists(os.path.dirname(run_whole)):
					os.makedirs(os.path.dirname(run_whole))

				# Add the process to the pool
				if varout == 'mrro':
					# For total runoff, take subsurface runoff and add surface runoff
					runpath2 = runpath.replace('item8235_monthly_mean','item8234_monthly_mean')
					process_run_add(runpath,runpath2,varin,varout,run_whole)
				else:
					process_run(runpath,varin,varout,run_whole,data_freq)

		# close the pool and make sure the processing has finished
		pool.close()
		pool.join()

	

	except Exception,e:
		print 'Error in script: '
		print e
		raise

#########################################################################
# Main function to set up variables and call 'process_data' worker function
#
if __name__=='__main__':
	host=socket.gethostname()

	# Set input and output paths
	#
	if host == 'happi.ggy.bris.ac.uk':
		basepath = '/data/scratch/pu17449/extracted_data/'
		baseout = '/data/scratch/pu17449/hadam3p_reformatted/HadAM3P/'
	elif host == 'anthropocene.ggy.bris.ac.uk':
		basepath = '/export/anthropocene/array-01/pu17449/extracted_data/EU25/'
		baseout = '/export/anthropocene/array-01/pu17449/happi_data_extra/HadAM3P-EU25'

	# Set experiments to process
	# 
	experiments = ['batch_518','batch_519','batch_520','batch_696']
	#experiments = ['batch_519','batch_520']
	#experiments = ['batch_646','batch_647','batch_648','batch_709']

	# Dictionary to match batches with HAPPI experiment names
	# 
	exp_map = {'batch_518':'All-Hist','batch_520':'Plus15-Future','batch_519':'Plus20-Future','batch_553':'All-Hist','batch_554':'All-Hist','batch_555':'All-Hist','batch_696':'Plus30-Future'}
#	exp_map = {'batch_646':'All-Hist','batch_647':'Plus15-Future','batch_648':'Plus20-Future','batch_709':'All-Hist'} # For wah_eu runs

	# Dictionary of standard variable names to match with hadam3p item codes
	#
	var_rename={'item3236_daily_maximum':'tasmax',
		'item3236_daily_minimum':'tasmin',
		'item3236_daily_mean':'tas',
		'item3236_monthly_mean':'tas',
		'item5216_daily_mean':'pr',
		'item5216_monthly_mean':'pr',
		'item8234_monthly_mean':'mrros',
		'item8235_monthly_mean':'mrro',
		'item3234_monthly_mean':'hfls',
		'item15201_daily_mean':'ua',
		'item15202_daily_mean':'va',
		'item1201_monthly_mean':'rsds',
		}

	# est and ver codes (for output file paths)
	#
	est = 'est1'
	ver = 'v1-2'

	# Domain for weather@home input data file paths
	# Either 'atmos' or 'region', could also include subset coordinates from extract script
	# Note OUTPUT domain is hardcoded to 'atmos' or 'land'
	# 
	domain = 'atmos'

	# Set variables to process
	# 
#	varlist =['item5216_daily_mean','item8234_monthly_mean','item8235_monthly_mean','item3234_monthly_mean',  'item3236_daily_maximum','item3236_daily_minimum','item3236_daily_mean','item3236_monthly_mean']
	#varlist = ['item8235_monthly_mean']
	# Daily variables
	varlist =  ['item3236_daily_maximum','item3236_daily_minimum','item3236_daily_mean', 'item5216_daily_mean','item15201_daily_mean','item15202_daily_mean']
	# Monthly variables
	varlist += ['item5216_monthly_mean','item3236_monthly_mean','item1201_monthly_mean']

	# For multi threading set number of processes to use
	numthreads = 5

	#########################################################################
	# Loop over experiments and variables then process them!
	#
	for exp_in in experiments:
		# set some experiment specific things
		exp_out = exp_map[exp_in]
		# For bias correction runs, use est2
		if exp_in in ['batch_709','batch_553','batch_555']:
			est = 'est2' 

		# Loop over variables
		for var_in in varlist:
			var_out = var_rename[var_in]
			process_data(exp_in,exp_out,var_in,var_out,basepath,numthreads,domain,est,ver)

