from netCDF4 import Dataset
import os,shutil,stat
import numpy as np

datadir = '/export/anthropocene/array-01/pu17449/happi_data'
#f_template = os.path.join(datadir,'landmask_template.nc')
f_template = os.path.join(datadir,'dodgey_data/sftlf_fx_HadAM3P-N96_All-Hist_est1_v1-0_run0_000000-000000.nc')
f_data = os.path.join(datadir,'HadAM3P/All-Hist/est1/v1-1/mon/land/mrro/runa001/HadAM3P_mrro_All-Hist_a001_2006-01_2015-12.nc')
f_out = os.path.join(datadir,'landmask/sftlf_fx_HadAM3P_All-Hist_est1_v2-0_run0000_000000-000000.nc')
print 'template:',f_template
print 'datafile:',f_data

with Dataset(f_data,'r') as f:
	lons = f.variables['longitude0'][:]
	lats = f.variables['latitude0'][:]
	mask = f.variables['mrro'][0,0,:].mask
	
sftlf = np.logical_not(mask)*100.
	
# Copy template to output file we will modify
shutil.copy(f_template,f_out)
os.chmod(f_out,0644)

with Dataset(f_out,'a') as f:
	f.variables['lat'][:]=lats
	f.variables['lon'][:]=lons
	f.variables['sftlf'][:]=sftlf
	del(f.institution)
	del(f.institute_id)
	f.__setattr__('contact','peter.uhe@bristol.ac.uk')
	f.__setattr__('model_id','HadAM3P')
	hist = f.history
	f.__setattr__('history','created by hadam3p_mask_file.py: HadAM3P mask copied from mask from runoff variable, using template sftlf file \n '+hist)
	
	
	