# Script to rearrange folders, taking in files in flat directory and 
# create a full folder structure for the HAPPI data, based on the file name
# 28/06/2018
# Peter Uhe
# 

import glob,os,sys

# Input and output directories
basein='/export/anthropocene/array-01/pu17449/happi_data/download'
baseout='/export/anthropocene/array-01/pu17449/happi_data/'


# Dictionaries to determine correct frequency and domain based on the mip table
freq_map = {'Lday':'day','Aday':'day','Amon':'mon','Lmon':'mon'}
domain_map ={'Lday':'land','Aday':'atmos','Amon':'atmos','Lmon':'land'}

for fpath in glob.glob(basein+'/*.nc'):
	fname = os.path.basename(fpath)
	var,mtable,model,expt,est,ver,run,timstr = fname.split('_')
	
	dir_out = os.path.join(baseout,model,expt,est,ver,freq_map[mtable],domain_map[mtable],var,run)
	if not os.path.exists(dir_out):
		os.makedirs(dir_out)
	fout = os.path.join(dir_out,fname)
	print fpath,fout
	os.rename(fpath,fout)
