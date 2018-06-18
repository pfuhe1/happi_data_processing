#!/usr/env/python
import sys,subprocess

#Monthly data:
#includestr="--include='pr_Amon*.nc' --include='tas*_Amon*.nc' --include='rsds_Amon*.nc' --include='hfls_Amon*.nc' --include='mrro*_Lmon*.nc'"
#includestr="--include='pr_Amon*.nc'"

#includearr=["--include='ua_Amon*.nc'","--include='va_Amon*.nc'","--include='zg_Amon*.nc'","--include='ps_Amon*.nc'","--include='psl_Amon*.nc'"]

includearr=["--include=ua_Amon*.nc","--include=va_Amon*.nc","--include=zg_Amon*.nc","--include=ps_Amon*.nc","--include=psl_Amon*.nc"]


#Daily data:
#includestr="--include='pr_Aday*.nc' --include='tasmax_Aday*.nc' --include='tasmin_Aday*.nc'"
#includestr="--include='pr_Aday*.nc' --include='rsds_Aday*.nc' --include='tas*_Aday*.nc'"
#includestr="--include='tasmax_Aday*.nc'"
#includestr="--include='tasmin_Aday*.nc'"
# Runoff
#includestr="--include='mrro_Lmon*.nc' --include='mrros_Lmon*.nc'"
# Latent heat flux
#includestr="--include='hfls_Amon*.nc'"

print 'include',includearr
#basedir='/export/silurian/array-01/pu17449/happi_data/'
#basedir='/export/triassic/array-01/pu17449/happi_data_monthly/'
#basedir='/data/scratch/happi_data/'
basedir='/export/anthropocene/array-01/pu17449/happi_data/'

models_list=['MIROC/MIROC5','NCC/NorESM1-HAPPI','CCCma/CanAM4','ETH/CAM4-2degree','MPI-M/ECHAM6-3-LR']
#models_list = ['CCCma/CanAM4']
#models_list=['ETH/CAM4-2degree']
#models_list=['MIROC/MIROC5','NCC/NorESM1-HAPPI','CCCma/CanAM4','ETH/CAM4-2degree']
#models_list=['NCC/NorESM1-HAPPI']
#models_list = ['LBNL/CAM5-1-2-025degree']


calls=[]
for model in models_list:
	print model
	flog = 'logs/cori_rsync_'+model.split('/')[-1]+'.log'
	rsync_cmd=["rsync","-mvruz","--include=*/"]+includearr+["--exclude=*","cori:/global/project/projectdirs/m1517/C20C/"+model,basedir]
	print rsync_cmd
	calls.append(subprocess.Popen(rsync_cmd,stdout=open(flog,'w'),stderr=subprocess.STDOUT))

for i,call in enumerate(calls):
	print 'waiting ',i,models_list[i]
	call.wait()
print 'done'

