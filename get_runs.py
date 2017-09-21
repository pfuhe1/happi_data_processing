# Functions  to get lists of HAPPI runs
import glob,os

# Choose runs 1-50 and 111-160 for MIROC Hist
def miroc_histruns(basepath,model,experiment,var,data_freq):
	runs = []
	for run in range(1,51):
		runs.append(basepath+model+'/'+experiment+ '/est1/v2-0/'+data_freq+'/atmos/'+ var+'/run'+str(run).zfill(3))
	for run in range(111,161):
		runs.append(basepath+model+'/'+experiment+ '/est1/v2-0/'+data_freq+'/atmos/'+ var+'/run'+str(run).zfill(3))
	return runs

# Choose runs 1-125 for NorESM1-HAPPI Hist
def norESM_histruns(basepath,model,experiment,var,data_freq):
	runs = []
	for run in range(1,126):
		runs.append(basepath+model+'/'+experiment+ '/est1/v1-0/'+data_freq+'/atmos/'+ var+'/run'+str(run).zfill(3))
	return runs

def CMIP5_runs(basepath,experiment,data_freq,var):
	runpath = os.path.join(basepath,var,experiment,var+'_'+data_freq+'_*.nc')
	return glob.glob(runpath)

def CESM_runs(basepath,experiment,data_freq,var):
	print 'getting runs for CESM'
#	basepath = '/export/silurian/array-01/pu17449/CESM_low_warming/decade_data_v2/'
	runpath = os.path.join(basepath,experiment,data_freq,var,'run*')
	return glob.glob(runpath)

# Get list of runs for a particular model, experiment, variable
def get_runs(model,experiment,basepath,data_freq,var):
	run_pattern = None
	if model =='CESM' or model == 'CESM-CAM5':
		return CESM_runs(basepath,experiment,data_freq,var)
	if model=='MIROC5' and experiment=='All-Hist':
		# choose specific runs
		return miroc_histruns(basepath,model,experiment,var,data_freq)
	elif model=='NorESM1-HAPPI' and experiment=='All-Hist':
		# choose specific runs
		return norESM_histruns(basepath,model,experiment,var,data_freq)
	elif model=='CAM4-2degree':
		run_pattern = 'ens0*'
	elif model=='CanAM4':
		run_pattern = 'r*i1p1'
	elif model == 'CMIP5':
		return CMIP5_runs(basepath,experiment,data_freq,var)
	else: 
	#Default
	# model=='MIROC5' or model=='NorESM1-HAPPI' or model=='HadAM3P' or model=='CAM5-1-2-025degree': 
		run_pattern = 'run*'

	# Set version if necessary:
	version = '*'
	if model == 'CAM5-1-2-025degree' and experiment == 'All-Hist':
		#Choose runs from v1-0 (not v1-0-aero) for CAM5-1-2-025 Hist
		version = 'v1-0'
	elif model == 'CAM4-2degree' and experiment[:4]=='Plus':
		# Choose version v2-0 for CAM4 future runs
		version = 'v2-0'

	if run_pattern:
		pathpattern=basepath+model+'/'+experiment+ '/*/'+version+'/'+data_freq+'/atmos/'+ var+'/'+run_pattern
		print pathpattern
		runs = glob.glob(pathpattern)
	return runs
