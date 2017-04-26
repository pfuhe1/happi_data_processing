# Script to do monthly means then ensemble average of HAPPI data
import os,glob,tempfile,shutil

# Method with cat first then monmean:
#real	0m21.483s
#user	0m17.232s
#sys	0m3.880s

# Method with monmean first


#temp_dir = tempfile.mkdtemp(dir=os.environ['HOME'])
temp_dir = tempfile.mkdtemp(dir='/export/silurian/array-01/pu17449/')

def list_to_string(l):
	s = ''
	for item in l:
		s += item +' '
	return s

if __name__=='__main__':

	try:
		run_averages = ''

		for run in range(1,5):
			run_files=glob.glob('/export/silurian/array-01/dm16883/Data_models/Raw/MIROC/Hist/Daily/tasmax/run'+str(run).zfill(3)+'/*.nc')
			run_mean = os.path.join(temp_dir,'run'+str(run).zfill(3)+'.nc')
			tmpfile=os.path.join(temp_dir+'/tmp.nc')
			# CDO commands
			cdo_cmd = 'cdo cat '+ list_to_string(run_files)+tmpfile
			print cdo_cmd
			os.system(cdo_cmd)
			cdo_cmd = 'cdo monmean ' + tmpfile+' '+run_mean
			print cdo_cmd
			os.system(cdo_cmd)
			run_averages += run_mean +' '
			os.remove(tmpfile)

		# Ensemble mean
		cdo_cmd = 'cdo ensmean ' + run_averages + 'ensmean_v2.nc'
		print cdo_cmd
		os.system(cdo_cmd)
		# Ensemble stdev
		cdo_cmd = 'cdo ensstd ' + run_averages + 'ensstd_v2.nc'
		print cdo_cmd
		os.system(cdo_cmd)

	except Exception,e:
		print 'Error in script: '
		print e
	finally:
		shutil.rmtree(temp_dir)
