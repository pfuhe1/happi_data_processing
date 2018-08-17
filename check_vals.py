import glob,os
from netCDF4 import Dataset
var = os.path.basename(os.getcwd())
for f in glob.glob('*/*.nc'):
	with Dataset(f,'r') as fnc:
		print os.path.basename(f),fnc.variables[var][:].mean()
