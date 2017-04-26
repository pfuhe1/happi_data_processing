# Script to do monthly means then ensemble average of HAPPI data
import os,glob,tempfile,shutil

# Method with cat first then monmean:
#real	0m21.483s
#user	0m17.232s
#sys	0m3.880s

# Method with monmean first then cat (less IO)
# real	0m11.456s
# user	0m11.034s
# sys	0m0.733s


#temp_dir = tempfile.mkdtemp(dir=os.environ['HOME'])
temp_dir = tempfile.mkdtemp(dir='/export/silurian/array-01/pu17449/')

def list_to_string(l):
	s = ''
	for item in l:
		s += '-monmean '+ item +' '
#		s += item +' '
	return s

if __name__=='__main__':

	model = 'MIROC'
#	experiments = ['Hist','2P0']
	experiments = ['1P5']
	var = 'tasmax'
	try:
		for experiment in experiments:
			run_averages = ''
			for run in range(1,51):
				#fnames = '/export/silurian/array-01/dm16883/Data_models/Raw/'+model+'/'+experiment+'/Daily/'+var+'/run'+str(run).zfill(3)+'/*.nc'
				fnames = '/export/silurian/array-01/dm16883/Data_models/Raw/'+model+'/'+experiment+ '/'+ var+'/run'+str(run).zfill(3)+'/*.nc'
				run_files=glob.glob(fnames)
				if len(run_files)==0:
					raise Exception('error, no files found: '+fnames)
				run_whole = os.path.join(temp_dir,'run'+str(run).zfill(3)+'.nc')
	#			tmpfile=os.path.join(temp_dir+'/tmp.nc')
				# CDO commands
				cdo_cmd = 'cdo cat '+ list_to_string(run_files)+run_whole
				print cdo_cmd
				os.system(cdo_cmd)
	#			cdo_cmd = 'cdo monmean ' + tmpfile+' '+run_whole
	#			print cdo_cmd
	#			os.system(cdo_cmd)
				run_averages += run_whole +' '
				#os.remove(tmpfile)

			# Ensemble mean
			outfile = 'test_output/'+model+'.'+var+'.'+experiment+'_ensmean.nc'
			cdo_cmd = 'cdo ensmean ' + run_averages + outfile
			print cdo_cmd
			os.system(cdo_cmd)

			# Ensemble stdev
			outfile = 'test_output/'+model+'.'+var+'.'+experiment+'_ensstd.nc'
			cdo_cmd = 'cdo ensstd ' + run_averages + outfile
			print cdo_cmd
			os.system(cdo_cmd)
			
			# Clean up files
			os.system('rm '+run_averages)

	except Exception,e:
		print 'Error in script: '
		print e
	finally:
		shutil.rmtree(temp_dir)
