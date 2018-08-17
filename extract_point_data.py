# Script to do monthly means then ensemble average of HAPPI data
import os,sys,glob,tempfile,shutil
import multiprocessing,time
import socket
from get_runs import get_runs
from netCDF4 import Dataset,MFDataset
import numpy as np
import argparse


######################################################################################
#
# Function to create netcdf file for single point, for many ensemble members
#
def create_netcdf_point(template,var,data,outname,y,x):
	if type(template)==type('') or type(template)==type([]): # template is a string
		#print template
		template = MFDataset(template,'r') # Read file

	# create outfile object
	outfile=Dataset(outname,'w',format ='NETCDF4_CLASSIC')

	# Create dimensions copied from template file
	temp=template.variables[var]

	for dim in temp.dimensions:
		if dim[:3]=='lat': 
			outfile.createDimension('lat',1)
			outfile.createVariable('lat','d',('lat',))
			outfile.variables['lat'][:]=template.variables[dim][y:y+1]
			for att in template.variables[dim].ncattrs():
				outfile.variables['lat'].__setattr__(att,template.variables[dim].__getattribute__(att))
		elif dim[:3] =='lon':
			outfile.createDimension('lon',1)
			outfile.createVariable('lon','d',('lon',))
			outfile.variables['lon'][:]=template.variables[dim][x:x+1]
			for att in template.variables[dim].ncattrs():
				outfile.variables['lon'].__setattr__(att,template.variables[dim].__getattribute__(att))
		if dim[:4] == 'time':
			outfile.createDimension('time',None)
			outfile.createVariable('time','d',('time',))
			outfile.variables['time'][:]=template.variables[dim][:] # Only make 1 timesteps
			for att in template.variables[dim].ncattrs():
				outfile.variables['time'].__setattr__(att,template.variables[dim].__getattribute__(att))
		
			#print template.variables[dim].__dict__

	print 'Created variables for lat and lon:',outfile.variables['lat'][0],outfile.variables['lon'][0]

	# Create dimension for 'n'
	outfile.createDimension('ens',data.shape[0])
	outfile.createVariable('ens','i',('ens',))
	outfile.variables['ens'][:]=np.arange(data.shape[0])
	outfile.variables['ens'].__setattr__('long_name','ensemble member')
	outfile.variables['ens'].__setattr__('comment','unitless list of ensemble members (not in any particular order)')
	
	# Create data variable 
	outfile.createVariable(var,'f',['ens','time','lat','lon'])
	for att in temp.ncattrs():
		if not att=='_FillValue':
			outfile.variables[var].__setattr__(att,template.variables[var].__getattribute__(att))
	
	# Override these attributes
	outfile.variables[var].__setattr__('units','mm/day')
	# Write data (and convert to mm/day)
	outfile.variables[var][:]=data*86400.
	
	if template.variables.has_key('lat_bnds'):
		outfile.createDimension('bnds',2)
		outfile.createVariable('lat_bnds','d',['lat','bnds'])
		outfile.createVariable('lon_bnds','d',['lon','bnds'])
		outfile.variables['lat_bnds'][:]=template.variables['lat_bnds'][y:y+1,:]
		outfile.variables['lon_bnds'][:]=template.variables['lon_bnds'][x:x+1,:]

#	if template.variables.has_key('time_bnds'):
#		if not 'bnds' in outfile.dimensions:
#			outfile.createDimension('bnds',2)
#		outfile.createVariable('time_bnds','d',['time','bnds'])
#		outfile.variables['time_bnds'][1,:]=template.variables['time_bnds'][:]

#	Copy attributes 
#
	for att in template.ncattrs():
		outfile.__setattr__(att,getattr(template,att))

# Add extra history information to output file
#
#	outfile.__setattr__('history',getattr(outfile,'history') + '\n Precip for closest point to dublin, calculated by P Uhe')
	
	outfile.close()
	print 'Finished creating data file',outname
	

######################################################################################
#
def extract_precip(run,model,y,x):
	print os.path.dirname(run[0])
	if len(run)==1:
		tmp = Dataset(run[0]).variables['pr']
	else:
		tmp = MFDataset(run).variables['pr']
	if len(tmp.shape)==4:
		return tmp[:,0,y,x]
	elif len(tmp.shape)==3:
		return tmp[:,y,x]
	else:
		raise Exception('unexpected shape of data')

######################################################################################
#
# Process all the data for the particular model, experiment and variable
#
def process_data(model,experiment,var,basepath,numthreads,data_freq,x,y,location_name,max_ensembles):

		# Create pool of processes to process runs in parallel. 
		pool = multiprocessing.Pool(processes=numthreads)


		outpath_runs=os.path.join(outdir,model,experiment)
		if not os.path.exists(outpath_runs):
			os.makedirs(outpath_runs)

		run_whole = os.path.join(outpath_runs,model+'_'+experiment+'_pr_'+location_name+'.nc')

		if not os.path.exists(run_whole):
			try:
				print 'Extracting data for:',run_whole
		
				# Get list of runs
				runs = get_runs(model,experiment,basepath,data_freq,var)
				# Limit runs if necessary
				runs = runs[:max_ensembles]

				result_all = ['']*len(runs)

				# Loop over runs
				for i,runpath in enumerate(runs): #test
					run = os.path.basename(runpath)
					runfiles = glob.glob(runpath+'/*.nc')

					# Don't use final year from miroc
					if model == 'MIROC5' and (experiment=='All-Hist' or experiment == 'Plus20-Future'):
						runfiles = runfiles[:-1]
					# Calculate ymonmean
				
					# Add the process to the pool
					result_all[i] = pool.apply_async(extract_precip,(runfiles,model,y,x))
					#result_all[i] = extract_precip(runfiles,model,y,x)
					#print model,experiment,var,runpath
					#testrun(runfiles,model)

				# close the pool and make sure the processing has finished
				pool.close()
				pool.join()

				data_all = []
#				for tmp in result_all:
				for i,result in enumerate(result_all):
					tmp = result.get(timeout=30.)
					if not tmp.sum()==0.0:
						data_all.append(tmp)
					else:
						print 'Dodgey data',runs[i]
			
				data_all = np.array(data_all)
				print data_all.shape
		
				template = runfiles # Template is just the files from the last ensemble processed
				create_netcdf_point(template,var,data_all,run_whole,y,x)
			except:
				if os.path.exists(run_whole):
					os.remove(run_whole)
				raise
		else:
			print 'files exists',run_whole


######################################################################################
#
# Main script
#
if __name__=='__main__':

	host=socket.gethostname()
	# For now only use data on anthropocene
	if host == 'anthropocene.ggy.bris.ac.uk':
		basepath = '/export/anthropocene/array-01/pu17449/happi_data/'
		numthreads=5

	# Set up command line argument parser
	parser=argparse.ArgumentParser('Script to extract point data :')
	out_dir_help='Base of output directory for extracted files'
	parser.add_argument('-o','--out_dir',required=True,help=out_dir_help)
	parser.add_argument('-x',required=True,type=int,help='grid index of longitude')
	parser.add_argument('-y',required=True,type=int,help='grid index of longitude')
	parser.add_argument('-m','--model',required=True,help = "model: options are 'NorESM1-HAPPI','MIROC5','CanAM4','CAM4-2degree','HadAM3P','ECHAM6-3-LR'")
	parser.add_argument('--name',default='point',help='name for location')
	parser.add_argument('--max_ensembles',default=100,type=int,help='Maximum ensembles to include from each scenario')

	# Get arguments
	args = parser.parse_args()
	outdir=args.out_dir
	x = args.x
	y = args.y
	model = args.model
	max_ensembles = args.max_ensembles
	location_name = args.name
	
	# Test arguments:
	# Get model, outdir, x, y from command line arguments
	# models available: ['NorESM1-HAPPI','MIROC5','CanAM4','CAM4-2degree','HadAM3P','ECHAM6-3-LR']
	#model = 'MIROC5'
	#outdir = '/export/anthropocene/array-01/pu17449/tmp'
	#x = 50
	#y = 50
	#location_name = 'point'
	#max_ensembles = 5
	
	experiments = ['All-Hist','Plus15-Future','Plus20-Future']
	varlist = ['pr',]#'tasmin','tasmax','rsds','tas']
	data_freq = {'pr':'day','tasmin':'day','tasmax':'day','tas':'mon','rsds':'mon'}

	# Looop over experiments and variables
	# Note, for now only 'pr' will work. 
	for experiment in experiments:
		for var in varlist:
			process_data(model,experiment,var,basepath,numthreads,data_freq[var],x,y,location_name,max_ensembles)


