# Functions  to get lists of HAPPI runs
import glob

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

# Get list of runs for a particular model, experiment, variable
def get_runs(model,experiment,basepath,data_freq,var):
	run_pattern = None
	if model=='MIROC5' and experiment=='All-Hist':
		# choose specific runs
		runs = miroc_histruns(basepath,model,experiment,var,data_freq)
	elif model=='NorESM1-HAPPI' and experiment=='All-Hist':
		# choose specific runs
		runs = norESM_histruns(basepath,model,experiment,var,data_freq)
	elif model=='CAM4-2degree':
		run_pattern = 'ens0*'
	elif model=='CanAM4':
		run_pattern = 'r*i1p1'
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