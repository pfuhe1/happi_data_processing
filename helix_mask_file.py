from netCDF4 import Dataset
import os,shutil,stat
import numpy as np

datadir = '/home/users/pfu599/helix_landfrac/'
#f_template = os.path.join(datadir,'landmask_template.nc')
f_data = 'mrlsl_day_HadGEM3_IPSL-CM5A-LR_r1i1p1_19680901_depthave.nc'

var_in = 'mrlsl'


f_data = os.path.join(datadir,f_data)
f_out = os.path.join(datadir,'sftlf_fx_HadGEM3.nc')
print 'datafile:',f_data

shutil.copy(f_data,f_out)

with Dataset(f_out,'a') as f:
        mask = f.variables[var_in][:].mask
        del(f.variables[var_in])

        sftlf = np.logical_not(mask)*100.

        f.createVariable('sftlf',np.float,('lat','lon'))
        f.variables['sftlf'][:]=sftlf
        f.variables['sftlf'].standard_name = 'land_area_fraction'
        f.variables['sftlf'].long_name = 'Land Area Fraction'
        f.variables['sftlf'].units = '%'

        hist = f.history
        f.__setattr__('history','created by helix_mask_file.py: HadGEM3 mask copied from missing value in '+var_in+' and output to sftlf variable\n '+hist)

