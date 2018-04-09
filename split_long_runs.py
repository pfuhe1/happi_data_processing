import os,sys,glob

indir = '/export/anthropocene/array-01/pu17449//happi_data_regrid/NorESM1-HAPPI/'
f1 = indir+'pr_day_NorESM1-HAPPI_All-Hist_run126_19850101-20151231.nc'

for decade in [1985,1991,2001,2011]:
	# Do separate files for each decade
	eyear = min((decade/10+1)*10,2015)
	print decade,eyear
	fout = indir+'pr_day_NorESM1-HAPPI_All-Hist_run126_'+str(decade)+'0101-'+str(eyear)+'1231.nc'
	cmd = 'cdo seldate,'+str(decade)+'-01-01,'+str(eyear)+'-12-31 '+f1+' '+fout

	print cmd
	os.system(cmd)
