# Script to do monthly means then ensemble average of HAPPI data
import os,glob,tempfile,shutil
import multiprocessing,time
from netCDF4 import Dataset
import socket
import subprocess
import time

# Get mappings:
# exp_map: map batch names (e.g. batch_509) to experiment names (e.g. All-Hist)
# var_rename: map item_code (e.g. item3236_monthly_mean) to standard short name (e.g. tas)
from wah_exp_var_mappings import exp_map,var_rename

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
	cdo_ret = subprocess.call(cdo_cmd)
	if cdo_ret !=0:
		raise IOError('Error in cdo command')

	# Rename variables and dimensions
	rename_vars(run_whole,varin,varout)

#########################################################################
# Process data for a single ensemble member/ run (mean of two fields)
#
def process_run_mean(runpath1,runpath2,varin,varout,run_whole):
	run = os.path.basename(runpath1)
	print run

	# CDO command
	cdo_cmd = ['cdo','-L','-divc,2', '-add', runpath1, runpath2, run_whole ]
	print cdo_cmd
	cdo_ret = subprocess.call(cdo_cmd)
	if cdo_ret !=0:
		raise IOError('Error in cdo command')

	# Hack for renaming tas
	if varin == 'item3236_daily_mean':
		varin = 'item3236_daily_minimum'

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
	
	print exp_in,varin
	print exp_out,varout

	# Create pool of processes to process runs in parallel. 
	pool = multiprocessing.Pool(processes=numthreads)

	# Get list of runs
	runstring = basepath+'/'+exp_in+'/'+domain+'/'+varin+'/'+varin+'*.nc'

	# Hack for calculating tas from tasmin, tasmax
	# Comment this out if we actually have output daily tas!
	if varout == 'tas' and data_freq == 'day':
		runstring = runstring.replace('item3236_daily_mean','item3236_daily_minimum')
	
	print runstring
	runs = glob.glob(runstring)
	# Allow time for partially created files to finish being created/copied
	time.sleep(1)
	outpath_runs = os.path.join(baseout,exp_out,est,ver,data_freq,domain_out,varout)

	# loop over runs to process
	#
	for runpath in runs:
		try:
			
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
				elif varout == 'tas' and data_freq == 'day': 
					# Tas is not available so approximate as mean of tasmin and tasmax
					# Comment this out if we actually have output daily tas!
					runpath2 = runpath.replace('item3236_daily_minimum','item3236_daily_maximum')
					process_run_mean(runpath,runpath2,varin,varout,run_whole)
				else:
					process_run(runpath,varin,varout,run_whole,data_freq)
		except IOError as e:
			print 'IOerror:'
			print e
		except Exception as e:
			print 'Error in script: '
			print e
			raise # if it is an unknown error exit the script completely

		# close the pool and make sure the processing has finished
		pool.close()
		pool.join()



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
		basepath = '/export/anthropocene/array-01/pu17449/extracted_data/SAS50/'
		baseout = '/export/anthropocene/array-01/pu17449/happi_data_extra/HadRM3P-SAS50'

	# Set experiments to process
	# 
	#experiments = ['batch_518','batch_519','batch_520','batch_696']
	experiments = ['batch_633',] #'batch_634','batch_635',,'batch_697','batch_535']
	#experiments = ['batch_519','batch_520']
	#experiments = ['batch_646','batch_647','batch_648','batch_709']


	# est and ver codes (for output file paths)
	#
	est = 'est1'
	ver = 'v1-0'

	# Domain for weather@home input data file paths
	# Either 'atmos' or 'region', 
	# could also include subset coordinates from extract script e.g. 'region_E55_N40_E105_N0'
	# Note OUTPUT domain is hardcoded to 'atmos' or 'land'
	# 
	#domain_in = 'atmos'
	domain_in = 'region_E55_N40_E105_N0'

	# Set variables to process
	# 
	varlist = None # Process all variables that are present in the input directory
	
#	varlist =['item5216_daily_mean','item8234_monthly_mean','item8235_monthly_mean','item3234_monthly_mean',  'item3236_daily_maximum','item3236_daily_minimum','item3236_daily_mean','item3236_monthly_mean']
	#varlist = ['item8235_monthly_mean']
	# Daily variables
	#varlist =  ['item3236_daily_maximum','item3236_daily_minimum','item3236_daily_mean', 'item5216_daily_mean','item15201_daily_mean','item15202_daily_mean']
	#varlist =  ['item3236_daily_maximum','item3236_daily_minimum','item3236_daily_mean', 'item5216_daily_mean',
	# Monthly variables
	#varlist += ['item5216_monthly_mean','item3236_monthly_mean','item1201_monthly_mean']
	#varlist += ['item3234_monthly_mean','item3236_monthly_mean','item1201_monthly_mean','item_15201_monthly_mean','item_15202_monthly_mean','item8234_monthly_mean']

	# For multi threading set number of processes to use
	numthreads = 5

	#########################################################################
	# Loop over experiments and variables then process them!
	#
	for exp_in in experiments:
		# set some experiment specific things
		exp_out = exp_map[exp_in]
		# For bias correction runs, use est2
		if exp_in in ['batch_709','batch_553','batch_554','batch_555','batch_697']:
			est1 = 'est2'
		else:
			est1 = est

		# Get variables in input folder instead of from list
		if varlist is None:
			varpaths = glob.glob(os.path.join(basepath,exp_in,domain_in,'*'))
			# Add daily tas (to compute from tasmin, tasmax)
			varpaths.append('item3236_daily_mean')
			# Loop over variable paths
			for varpath in varpaths:
				print varpath
				var_in = os.path.basename(varpath)
				var_out = var_rename[var_in]
				process_data(exp_in,exp_out,var_in,var_out,basepath,numthreads,domain_in,est1,ver)
		else:
			# Loop over variable list
			for var_in in varlist:
				var_out = var_rename[var_in]
				process_data(exp_in,exp_out,var_in,var_out,basepath,numthreads,domain_in,est1,ver)

