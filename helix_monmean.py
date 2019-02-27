# Script to create monthly means from helix data:
import os,sys,subprocess,glob
import numpy as np

basepath = '/work/scratch/pfu599/helix_data/SWL_data/'
inpaths = glob.glob(basepath+'/*/*/*/*/day')
for path1 in inpaths:
	for path2 in glob.glob(os.path.join(path1,'*/*/*/*.nc')):
		print path2
		outpath = path2.replace('day','mon')
		print outpath
		outdir = os.path.dirname(outpath)
		if not os.path.exists(outdir):
			os.makedirs(outdir)
		subprocess.call(['cdo','monmean',path2,outpath])

