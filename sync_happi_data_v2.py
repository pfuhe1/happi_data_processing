#!/usr/env/python
import sys,os

#Monthly data:
includestr="--include='pr_Amon*.nc' --include='tas*_Amon*.nc' --include='rsds_Amon*.nc' --include='hfls_Amon*.nc' --include='mrro*_Lmon*.nc'"
#includestr="--include='pr_Amon*.nc'"

#Daily data:
#includestr="--include='pr_Aday*.nc' --include='tasmax_Aday*.nc' --include='tasmin_Aday*.nc'"

#includestr="--include='tasmax_Aday*.nc'"
#includestr="--include='tasmin_Aday*.nc'"
# Runoff
#includestr="--include='mrros_Lmon*.nc'"
# Latent heat flux
#includestr="--include='hfls_Amon*.nc'"

print 'include',includestr
basedir='/data/scratch/happi_data/'
models_list=['MIROC/MIROC5','NCC/NorESM1-HAPPI','CCCma/CanAM4','ETH/CAM4-2degree']
#models_list=['NCC/NorESM1-HAPPI']
#models_list = ['LBNL/CAM5-1-2-025degree']
# 'LBNL/CAM5-1-2-025degree' # skipping for now (only 4-5 ensemble members anyway and I'm out of disk quotso I moved this to /export/triassic/array-01/pu17449/happi_data2/ ) 

for model in models_list:
	print model
	rsync_cmd="rsync -mvruz --include='*/' "+includestr+" --exclude='*' pfuhe1@cori.nersc.gov:/global/project/projectdirs/m1517/C20C/"+model+" "+basedir
	#rsync_cmd="rsync -mvruz --include='*/' "+includestr+" --exclude='*' pfuhe1@cori.nersc.gov:/global/project/projectdirs/m1517/C20C/"+model+"/All-Hist "+basedir+'/'+model.split('/')[-1]
	print rsync_cmd
	os.system(rsync_cmd)

