#!/usr/env/python
import sys,subprocess

# Function to make rsync include string based on list of include vars:
# Format of includevars is 'varname_CMIPtable' (cmip table specifies data frequency)
def includestr(includevars):
	includearr = []
	for var in includevars:
		includearr.append('--include='+var+'*.nc')
	return includearr

# Set up paths
local_basedir='/export/anthropocene/array-01/pu17449/happi_data/'
remote_basedir="cori:/global/project/projectdirs/m1517/C20C/"

# List models to sync
models_list=['MIROC/MIROC5','NCC/NorESM1-HAPPI','CCCma/CanAM4','ETH/CAM4-2degree','MPI-M/ECHAM6-3-LR','LBNL/CAM5-1-2-025degree']
#models_list = ['LBNL/CAM5-1-2-025degree']

# List variables to sync
#
includevars = []
#Monthly vars:
#includevars += ['pr_Amon','tas*_Amon','rsds_Amon','hfls_Amon','mrro*_Lmon','ua_Amon','va_Amon','zg_Amon','ps_Amon','psl_Amon','hurs_Amon']
#includevars+= ['wa_Amon','wap_Amon'] # testing download of vertical velocity
#includevars += ['pr_Amon','tas*_Amon','hfls_Amon','mrro*_Lmon','ua_Amon','va_Amon','zg_Amon','ps_Amon']

# Daily vars
#includevars += ['pr_Aday','rsds_Aday','tas*_Aday']
#includevars += ['pr_Aday','tas*_Aday','hurs_Aday']
includevars += ['psl_Aday']

# Convert includevars to rsync include string
includearr=includestr(includevars)
print 'include',includearr

# Loop over models and call rsync routine
calls=[]
for model in models_list:
	print model
	flog = 'logs/cori_rsync_'+model.split('/')[-1]+'.log'
	rsync_cmd=["rsync","-mvruz","--include=*/"]+includearr+["--exclude=*",remote_basedir+model,local_basedir]
	print rsync_cmd
	calls.append(subprocess.Popen(rsync_cmd,stdout=open(flog,'w'),stderr=subprocess.STDOUT))

# Make sure we wait for each call to complete before exiting
for i,call in enumerate(calls):
	print 'waiting ',i,models_list[i]
	call.wait()
print 'done'

