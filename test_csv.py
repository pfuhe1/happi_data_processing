import os
from netCDF4 import Dataset
import numpy as np
import time
import shutil

# new script to write csv data to netcdf
indatadir = '/export/anthropocene/array-01/pu17449/PHE'
intemplatedir = '/export/anthropocene/array-01/pu17449/isimip_bc/obs/EWEMBI-UK25'
infiles = ['GreaterManchester96_2013.csv','London96_2013.csv','Wmidlands96_2013.csv']
locs = ['manchester','london','birmingham']

for i,fname in enumerate(infiles):
	print locs[i]
	outdir = '/export/anthropocene/array-01/pu17449/isimip_bc/obs/UKCP09-'+locs[i]
	if not os.path.exists(outdir):
		os.makedirs(outdir)
	fpath = os.path.join(indatadir,fname)
	f_template = os.path.join(intemplatedir,'tas_day_EWEMBI-'+locs[i]+'_19960101-20131231.nc')
	f_out = os.path.join(outdir,'tas_day_UKCP09-'+locs[i]+'_19960101-20131231.nc')
	shutil.copy(f_template,f_out)
	#with open(fpath,'r') as fcsv:
	#	fcsv.readline() # read header line
	#	for line in fcsv:
	#		arr = line.split(',')
	#		date = arr[0]
	#		tmean = arr[1]
	tasdata = np.genfromtxt(fpath,skip_header=1,delimiter=',')[:,1]+273.15

	with Dataset(f_out,'a') as f:
		f.variables['tas'][:] = tasdata
		f.history = f.history+time.ctime()+': replaced EWEMBI data with UKCP09 data from '+fname
	