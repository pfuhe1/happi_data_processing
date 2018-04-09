import os,sys,glob

indir = '/export/anthropocene/array-01/pu17449/happi_data_extra/HadAM3P-EU25/All-Hist/est2/v1-0/day/atmos/tas'
outdir = '/export/anthropocene/array-01/pu17449/happi_data_extra/HadAM3P-EU25/All-Hist/est2/v1-0/day/atmos/tas'

cdo_cmd = 'cdo cat '
for year in range(1986,2014):
	pattern = indir+'/run*/*_'+str(year)+'-01_'+str(year)+'-12.nc'
	print pattern
	runs = glob.glob(pattern)
	cdo_cmd += runs[0]+' '

fout = indir+'/runcat1/tas_Aday_HadAM3P_All-Hist_est2_v1-0_runcat1_2017-01_2017-12.nc'
cdo_cmd += fout
print cdo_cmd
cdo_cmd = cdo_cmd.replace('tas','tasmax')
fout = fout.replace('tas','tasmax')
print cdo_cmd
print fout

if not os.path.exists(os.path.dirname(fout)):
	os.makedirs(os.path.dirname(fout))
#cmd1 = 'cdo cat '+f1+' '+f2+' tmp.nc'
#print cmd1
#os.system(cmd1)

#os.system(cdo_cmd)
