import os,sys,glob

indir = '/export/anthropocene/array-01/pu17449/happi_data/NorESM1-HAPPI/All-Hist/est1/v1-0/day/atmos/pr/run126'
outdir = '/export/anthropocene/array-01/pu17449/happi_data_long/NorESM1-HAPPI/All-Hist/est1/v1-0/day/atmos/pr/run126'
f1 = indir+'/pr_Aday_NorESM1-HAPPI_All-Hist_est1_v1-0_run126_19590101-19851231.nc'
f2 = indir+'/pr_Aday_NorESM1-HAPPI_All-Hist_est1_v1-0_run126_19860101-20160630.nc'
fout = outdir+'/pr_Aday_NorESM1-HAPPI_All-Hist_est1_v1-0_run126_19790101-20131231.nc'
if not os.path.exists(outdir):
	os.makedirs(outdir)
#cmd1 = 'cdo cat '+f1+' '+f2+' tmp.nc'
#print cmd1
#os.system(cmd1)

cmd2 = 'cdo seldate,1979-01-01,2015-12-31 tmp.nc '+fout
print cmd2
os.system(cmd2)
