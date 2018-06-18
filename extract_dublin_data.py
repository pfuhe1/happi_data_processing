# Script to do monthly means then ensemble average of HAPPI data
import os,sys,glob,tempfile,shutil
import multiprocessing,time
import socket
from get_runs import get_runs
from netCDF4 import Dataset,MFDataset
import numpy as np

sys.path.append('../cpdn_analysis')
from region_returntimes import find_closest_1d_v2


def get_dublinpt(model):
	maskdir = '/data/scratch/happi_data/landmask'
	d_lat  = 53.35
	d_lon =  -6.27
	
	if model !='HadAM3P':
		f_lm = glob.glob(maskdir+'/sftlf_fx_'+model+'*.nc')[0]
		tmp = Dataset(f_lm,'r')
		landfrac = tmp.variables['sftlf'][:]
		lon_coord = tmp.variables['lon'][:]
		lat_coord = tmp.variables['lat'][:]
	else:
		f_lm = '/data/scratch/happi_data/HadAM3P/hadam3p_formatted/All-Hist/mon/mrro/runa025/HadAM3P_mrro_All-Hist_a025_2006-01_2015-12.nc'
		tmp = Dataset(f_lm,'r')
		landfrac = np.logical_not(tmp.variables['mrro'][0,0,:].mask)
		lon_coord = tmp.variables['longitude0'][:]
		lat_coord = tmp.variables['latitude0'][:]

	y,x = find_closest_1d_v2(d_lat,d_lon,lat_coord,lon_coord)
	# Hack to avoid ocean points
	if model == 'MIROC5' or model == 'CAM4-2degree' or model == 'HadAM3P':
		x=x-1
	print 'mapped_coords2',lat_coord[y],lon_coord[x]-360
	print 'landfrac',landfrac[y,x]
	return y,x


def create_netcdf_dublin(template,var,data,outname,y,x):
	if type(template)==type('') or type(template)==type([]): # template is a string
		print template
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

	for att in template.ncattrs():
		outfile.__setattr__(att,getattr(template,att))
	outfile.__setattr__('history',getattr(outfile,'history') + '\n Precip for closest point to dublin, calculated by P Uhe')
	

	outfile.close()

def testrun(runpath,model):
	maskdir = '/data/scratch/happi_data/landmask'
	d_lat  = 53.35
	d_lon =  -6.27
	tmp = MFDataset(runpath)
	try:
		lon_coord = tmp.variables['lon'][:]
		lat_coord = tmp.variables['lat'][:]
	except:
		lon_coord = tmp.variables['longitude0'][:]
		lat_coord = tmp.variables['latitude0'][:]
		
	y,x = find_closest_1d_v2(d_lat,d_lon,lat_coord,lon_coord)
	print 'mapped_coords',lat_coord[y-1:y+2],lon_coord[x-1:x+2]-360
	if model == 'MIROC5' or model == 'CAM4-2degree' or model == 'HadAM3P':
		x2=x-1
	else:
		x2=x
	print 'mapped_coords2',lat_coord[y],lon_coord[x2]-360
	if model !='HadAM3P':
		f_lm = glob.glob(maskdir+'/sftlf_fx_'+model+'*.nc')[0]
		landfrac = Dataset(f_lm,'r').variables['sftlf'][:]
	else:
		f_lm = '/data/scratch/happi_data/HadAM3P/hadam3p_formatted/All-Hist/mon/mrro/runa025/HadAM3P_mrro_All-Hist_a025_2006-01_2015-12.nc'
		landfrac = np.logical_not(Dataset(f_lm,'r').variables['mrro'][0,0,:].mask)
		print landfrac.shape

	print 'landfrac',landfrac[y,x2]
	
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
	

# Process all the data for the particular model, experiment and variable
def process_data(model,experiment,var,basepath,numthreads,data_freq):

		y,x = get_dublinpt(model)

		# Create pool of processes to process runs in parallel. 
		pool = multiprocessing.Pool(processes=numthreads)


		outpath_runs=os.path.join(outdir,model,experiment)
		if not os.path.exists(outpath_runs):
			os.makedirs(outpath_runs)

		run_whole = os.path.join(outpath_runs,model+'_'+experiment+'_pr_dublin.nc')

		if not os.path.exists(run_whole):
			try:
				print run_whole
		
				# Get list of runs
				runs = get_runs(model,experiment,basepath,data_freq,var)

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
		#		for tmp in result_all:
				for i,result in enumerate(result_all):
					tmp = result.get(timeout=30.)
					if not tmp.sum()==0.0:
						data_all.append(tmp)
					else:
						print 'Dodgey data',runs[i]
			
				data_all = np.array(data_all)
				print data_all.shape
		
				template = runfiles
				create_netcdf_dublin(template,var,data_all,run_whole,y,x)
			except:
				if os.path.exists(run_whole):
					os.remove(run_whole)
				raise
		else:
			print 'files exists',run_whole

if __name__=='__main__':

	host=socket.gethostname()

	if host == 'happi.ggy.bris.ac.uk':
		basepath = '/data/scratch/happi_data/'
		models = ['NorESM1-HAPPI','MIROC5','CanAM4','CAM4-2degree','HadAM3P']
		numthreads=5
		outdir = '/data/scratch/pu17449/dublin_data/'

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


