# Functions  to get lists of HAPPI runs
import glob,os

######################################################################

# Choose runs 1-50 and 111-160 for MIROC Hist
def miroc_histruns(basepath,model,experiment,var,data_freq):
	runs = []
	for run in range(1,51):
		runs.append(basepath+model+'/'+experiment+ '/est1/v2-0/'+data_freq+'/atmos/'+ var+'/run'+str(run).zfill(3))
	for run in range(111,161):
		runs.append(basepath+model+'/'+experiment+ '/est1/v2-0/'+data_freq+'/atmos/'+ var+'/run'+str(run).zfill(3))
	return runs

######################################################################

# Choose runs 1-50 and 101-160 for MIROC Hist (including bc runs, but not runs startin gin 2010
def miroc_histruns_all(basepath,model,experiment,var,data_freq):
	runs = []
	for run in range(1,51):
		runs.append(basepath+model+'/'+experiment+ '/est1/v2-0/'+data_freq+'/atmos/'+ var+'/run'+str(run).zfill(3))
	for run in range(101,161):
		runs.append(basepath+model+'/'+experiment+ '/est1/v2-0/'+data_freq+'/atmos/'+ var+'/run'+str(run).zfill(3))
	return runs
	
######################################################################

# Choose runs 1-125 for NorESM1-HAPPI Hist
def norESM_histruns(basepath,model,experiment,var,data_freq,domain):
	runs = []
	for run in range(1,126):
		runs.append(basepath+model+'/'+experiment+ '/est1/v1-0/'+data_freq+'/'+domain+'/'+ var+'/run'+str(run).zfill(3))
	return runs

######################################################################

def CMIP5_runs(basepath,experiment,data_freq,var):
	runpath = os.path.join(basepath,var,experiment,var+'_'+data_freq+'_*.nc')
	return glob.glob(runpath)

######################################################################

def CESM_runs(basepath,experiment,data_freq,var):
	print 'getting runs for CESM'
#	basepath = '/export/silurian/array-01/pu17449/CESM_low_warming/decade_data_v2/'
	runpath = os.path.join(basepath,experiment,data_freq,var,'run*')
	#print runpath
	return glob.glob(runpath)

######################################################################

# Get list of runs for a particular model, experiment, variable
def get_runs(model,experiment,basepath,data_freq,var,domain='atmos'):
	run_pattern = None
	if model =='CESM' or model == 'CESM-CAM5':
		return CESM_runs(basepath,experiment,data_freq,var)
	if model=='MIROC5' and experiment=='All-Hist':
		# choose specific runs
		return miroc_histruns(basepath,model,experiment,var,data_freq)
	elif model=='NorESM1-HAPPI' and experiment=='All-Hist':
		# choose specific runs
		return norESM_histruns(basepath,model,experiment,var,data_freq,domain)
	elif model=='CAM4-2degree':
		run_pattern = 'ens0*'
	elif model=='CanAM4' or model=='ec-earth3-hr' or model == 'hadgem3':
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
	elif model == 'ECHAM6-3-LR' and experiment == 'All-Hist':
		version = 'v1-0'
	elif model == 'MIROC5' and experiment == 'Plus15-Future':
		# Incorrect data was replaced with v3-0
		version = 'v3-0'
	elif model == 'MIROC5' and experiment == 'Plus20-Future':
		# Just use v2-0, not additional 'Land' experiments
		version = 'v2-0'
	elif model == 'NorESM1-HAPPI' and experiment[:4]=='Plus':
		#use version 2 for NorESM future runs 
		version = 'v2-0'
	elif model == 'HadAM3P':
		# Use version v1-2 for HadAM3P 
		# (the data should be the same, but this was reprocessed with renamed dimensions)
		version = 'v1-2' 

	# set est (estimate) if necessary
	est = '*'
	if model == 'CanAM4' and experiment == 'All-Hist':
		est = 'est1' # Use est2 for bias correction runs
	elif model == 'HadAM3P' or model=='HadAM3P-EU25' or model=='HadRM3P-SAS50':
		est = 'est1' # Use est2 for bias correction runs


	# Get list of paths that match our filename pattern
	if run_pattern:
		pathpattern=basepath+model+'/'+experiment+ '/'+est+'/'+version+'/'+data_freq+'/'+domain+'/'+ var+'/'+run_pattern
		print pathpattern
		runs = glob.glob(pathpattern)
	return runs

######################################################################

# Get list of runs for a particular model, experiment, variable
# This gets all of the runs, including bias correction and standard short runs
def get_runs_all(model,experiment,basepath,data_freq,var,domain='atmos'):
	run_pattern = None
	if model =='CESM' or model == 'CESM-CAM5':
		return CESM_runs(basepath,experiment,data_freq,var)
	elif model=='MIROC5' and experiment=='All-Hist':
		# choose specific runs
		return miroc_histruns_all(basepath,model,experiment,var,data_freq)
	elif model=='CAM4-2degree':
		run_pattern = 'ens*'
	elif model=='CanAM4' or model.lower()=='ec-earth3-hr' or model.lower() == 'hadgem3':	
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
#	elif model == 'ECHAM6-3-LR' and experiment == 'All-Hist':
#		version = 'v1-0' # version 1 is short runs, version 1-1 is bias correction runs
	elif model == 'MIROC5' and experiment == 'Plus15-Future':
		# Incorrect data was replaced with v3-0
		version = 'v3-0'
	elif model == 'MIROC5' and experiment == 'Plus20-Future':
		# Just use v2-0, not additional 'Land' experiments
		version = 'v2-0'
	elif model == 'NorESM1-HAPPI' and experiment[:4]=='Plus':
		#use version 2 for NorESM future runs 
		version = 'v2-0'
	elif model == 'HadAM3P':
		# Use version v1-2 for HadAM3P 
		# (the data should be the same, but this was reprocessed with renamed dimensions)
		version = 'v1-2' 

	# set est (estimate) if necessary
	est = '*'
	if model == 'CanAM4' and experiment == 'All-Hist':
		est = 'est1' # Use est2 for bias correction runs
	elif model == 'HadAM3P' or model=='HadAM3P-EU25' or model=='HadRM3P-SAS50':
		est = 'est1' # Don't use bias correction runs for now

	# Get list of paths that match our filename pattern
	if run_pattern:
		pathpattern=basepath+model+'/'+experiment+ '/'+est+'/'+version+'/'+data_freq+'/'+domain+'/'+ var+'/'+run_pattern
		print pathpattern
		runs = glob.glob(pathpattern)
	return runs

##########################################################################################
# Bias correction runs

# Choose runs 1-50 and 111-160 for MIROC Hist
def miroc_bcruns(basepath,model,experiment,var,data_freq):
	runs = []
	for run in range(101,110):
		runs.append(basepath+model+'/'+experiment+ '/est1/v2-0/'+data_freq+'/atmos/'+ var+'/run'+str(run).zfill(3))
	return runs

######################################################################

# Choose runs 126-136 for NorESM1-HAPPI long bias correction runs
def norESM_bcruns(basepath,model,experiment,var,data_freq,domain):
	runs = []
	for run in range(126,136):
		runs.append(basepath+model+'/'+experiment+ '/est1/v1-0/'+data_freq+'/'+domain+'/'+ var+'/run'+str(run).zfill(3))
	return runs

######################################################################

# Get list of BIAS CORRECTION runs for a particular model, variable
def get_bc_runs(model,experiment,basepath,data_freq,var,domain='atmos'):
	experiment = 'All-Hist'
	run_pattern = None
	if model =='CESM' or model == 'CESM-CAM5':
		experiment = 'historical'
		return CESM_runs(basepath,experiment,data_freq,var)
	if model=='MIROC5':
		# choose specific runs
		return miroc_bcruns(basepath,model,experiment,var,data_freq)
	elif model=='NorESM1-HAPPI':
		# choose specific runs
		return norESM_bcruns(basepath,model,experiment,var,data_freq,domain)
	elif model=='CAM4-2degree':
		run_pattern = 'ens1*'
	elif model=='CanAM4':
		run_pattern = 'r*i1p1'
#		raise Exception('Need to determine bias correction runs for model:'+model)
	elif model == 'CMIP5':
#		return CMIP5_runs(basepath,experiment,data_freq,var)
		raise Exception('Need to determine bias correction runs for model:'+model)
	else: 
	#Default
	# model=='MIROC5' or model=='NorESM1-HAPPI' or model=='HadAM3P' or model=='CAM5-1-2-025degree': 
		run_pattern = 'run*'
		#raise Exception('Need to determine bias correction runs for model:'+model)

	# Set version if necessary:
	version = '*'
	if model == 'CAM5-1-2-025degree' and experiment == 'All-Hist':
		#Choose runs from v1-0 (not v1-0-aero) for CAM5-1-2-025 Hist
		version = 'v1-0'
	elif model =='ECHAM6-3-LR':
		version = 'v1-1'

	# set est (estimate) if necessary
	est = '*'
	if model == 'HadAM3P' or model=='HadAM3P-EU25' or model=='HadRM3p-SAS50':
		est = 'est2' # Use est2 for bias correction runs
# NOTE: for CanAM4 est2 actually points to amip runs from CMIP5 as there aren't any bias correction runs specifically for HAPPI
	if model =='CanAM4':
		est = 'est2' 
	if run_pattern:
		pathpattern=basepath+model+'/'+experiment+ '/'+est+'/'+version+'/'+data_freq+'/'+domain+'/'+ var+'/'+run_pattern
		print pathpattern
		runs = glob.glob(pathpattern)
	return runs
