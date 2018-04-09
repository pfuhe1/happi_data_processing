##################################
# Python script to run isimip bias correction code
# Peter Uhe 12/1/2018
# 
# Assumes that the bias correction transfer coefficients for the model/variable
# have already been calculated, and applies the bias correction to other runs

import sys,os,glob,subprocess,multiprocessing
sys.path.append('/home/bridge/pu17449/src/happi_data_processing')
from get_runs import get_runs

# Input parameters
model='NorESM1-HAPPI'
var='pr'
freq='day'
daterange_calibrate = '1979-2013'

#expt='All-Hist'
#daterange_app = '2006-2015'

expt='Plus20-Future'
daterange_app='2106-2115'

# Paths
wdir = '/export/anthropocene/array-01/pu17449/'
datadir = os.path.join(wdir,'happi_data/')
isimip_indir = os.path.join(wdir,'happi_data_long')
isimip_regriddir = os.path.join(wdir,'happi_data_regrid')
isimip_outdir = os.path.join(wdir,'happi_data_corrected')

# Multithreading
#numthreads=1

def bias_correct_run(model,expt,var,freq,daterange_calibrate,daterange_app,isimip_indir,isimip_regriddir,isimip_outdir,runpath):

	arr = os.path.dirname(runpath).split('/')
	ver = arr[-4]
	est = arr[-5]
	realization = os.path.basename(runpath)
	os.environ['realization'] = realization
	print 'realization',realization
	os.environ['est'] = est
	os.environ['ver'] = ver
	print 'est,ver',est,ver

	regrid_file = os.path.join(isimip_regriddir,model,var+'_'+freq+'_'+model+'_'+expt+'_'+realization+'_'+daterange_app[:4]+'0101-'+daterange_app[5:]+'1231.nc')

	final_file = os.path.join(isimip_outdir,model,'EWEMBI',var+'_'+freq+'_'+model+'_'+expt+'_'+realization+'_EWEMBI_'+daterange_app[:4]+'0101-'+daterange_app[5:]+'1231.nc')

	if os.path.exists(final_file):
		print 'Bias corrected file already exists, skipping:',final_file
		return

	##################################################################
	# 1) Select time from files and move to isimip input folder

	if os.path.isdir(runpath):
		# get input files in runpath
	 	run_files=sorted(glob.glob(runpath+'/*.nc'))
	else:
		# The runpath is the file
		run_files = [runpath]

	if len(run_files)==1:
		datelong = daterange_app[:4]+'0101,'+daterange_app[5:]+'1231'
		fname = run_files[0]
		fstem = os.path.basename(fname)[:-20]
		#fout = os.path.join(isimip_indir,fstem+daterange_app[:4]+'0101-'+daterange_app[5:]+'1231.nc')
		fdir = os.path.dirname(fname).replace('happi_data','happi_data_long')
		if not os.path.exists(fdir):
			os.makedirs(fdir)
		fout = os.path.join(fdir,fstem+daterange_app[:4]+'0101-'+daterange_app[5:]+'1231.nc')
		#cmd = 'cdo selyear,'+date+' '+' ' + run_files[0]+' '+fout
		cmdarr = ['cdo','seldate,'+datelong,run_files[0],fout]
	else:
		raise Exception('multiple input files, TODO need to concatenate these')


	#print cmd
	#os.system(cmd)
	print cmdarr
	proc = subprocess.Popen(cmdarr, stdout=subprocess.PIPE,stderr=subprocess.STDOUT)
	(out, err) = proc.communicate()
	print out

	##################################################################
	# 2) ISIMIP script: interpolate.2obsdatagrid.2prolepticgregoriancalendar

	flog = 'logs/regrid_'+model+'_'+expt+'_'+realization+'.log'
	cmd = './interpolate.2obsdatagrid.2prolepticgregoriancalendar.sh EWEMBI '+var+' '+model+' '+expt+' '+daterange_app
	print cmd
	#os.system(cmd)
	#cmdarr = ['./interpolate.2obsdatagrid.2prolepticgregoriancalendar.sh', 'EWEMBI',var,model,expt,daterange_app]
	#print cmdarr
	proc = subprocess.Popen(cmd, stdout=open(flog,'w'),stderr=subprocess.STDOUT,shell=True)
	proc.wait()
	
	if os.path.exists(regrid_file):
		print 'Finished regridding',regrid_file
	else:
		print 'Failed regridding',regrid_file


	##################################################################
	# 3) ISIMIP script: app.coef
	flog = 'logs/bc_'+model+'_'+expt+'_'+realization+'.log'
	cmd = './app.coef.sh EWEMBI '+daterange_calibrate+' '+var+' '+var+' '+model+' '+expt+' '+daterange_app
	print cmd
	#os.system(cmd)
	#cmdarr = ['./app.coef.sh','EWEMBI',daterange_calibrate,var,var,model,expt,daterange_app]
	#print cmdarr
	#proc = subprocess.Popen(cmd, stdout=subprocess.PIPE,stderr=subprocess.STDOUT,env={'realization':realization},shell=True,executable='/bin/bash')
	proc = subprocess.Popen(cmd, stdout=open(flog,'w'),stderr=subprocess.STDOUT,shell=True)
	proc.wait()

	if os.path.exists(final_file):
		print 'Bias corrected file successfully created:',final_file
	else:
		print 'Script finished but bias corrected file not created:',final_file


###########################################################################################
# Main script

f_runs = get_runs(model,expt,datadir,freq,var)
#realization='run001'

for runpath in f_runs[:20]:
	
	# Create pool of processes to process runs in parallel. 
	#pool = multiprocessing.Pool(processes=numthreads)	
	#pool.apply_async(bias_correct_run, (model,expt,var,freq,daterange_calibrate,daterange_app,isimip_indir,isimip_regriddir,isimip_outdir,runpath))

# Finish up
#pool.close()
#pool.join()
	bias_correct_run(model,expt,var,freq,daterange_calibrate,daterange_app,isimip_indir,isimip_regriddir,isimip_outdir,runpath)

	


