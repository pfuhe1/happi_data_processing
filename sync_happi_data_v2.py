#!/usr/env/python
import sys,os

#Monthly data:
includestr="--include='pr_Amon*.nc' --include='tas*_Amon*.nc' --include='rsds_Amon*.nc'"

#Daily data:
#includestr="--include='pr_Aday*.nc' --include='tasmax_Aday*.nc' --include='tasmin_Aday*.nc'"
#includestr="--include='pr_Aday*.nc' --include='rsds_Amon*.nc' --include='tas_Amon*.nc'"
#includestr="--include='tasmax_Aday*.nc'"
#includestr="--include='tasmin_Aday*.nc'"

print 'include',includestr
#basedir='/export/silurian/array-01/pu17449/happi_data/'
basedir='/export/triassic/array-01/pu17449/happi_data2/'
#models_list=['MIROC/MIROC5','NCC/NorESM1-HAPPI','CCCma/CanAM4','ETH/CAM4-2degree']
#models_list=['ETH/CAM4-2degree']
models_list = ['LBNL/CAM5-1-2-025degree']
# 'LBNL/CAM5-1-2-025degree' # skipping for now (only 4-5 ensemble members anyway and I'm out of disk quotso I moved this to /export/triassic/array-01/pu17449/happi_data2/ ) 

for model in models_list:
	print model
	rsync_cmd="rsync -mvruz --include='*/' "+includestr+" --exclude='*' pfuhe1@cori.nersc.gov:/global/project/projectdirs/m1517/C20C/"+model+" "+basedir
	print rsync_cmd
	os.system(rsync_cmd)

