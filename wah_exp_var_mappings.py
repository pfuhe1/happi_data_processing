###############################################################################
#
# Dictionary to match batches with HAPPI experiment names
 
exp_map = {}
# HadAM3P simulations (global)
exp_map['batch_518']='All-Hist'
exp_map['batch_520']='Plus15-Future'
exp_map['batch_519']='Plus20-Future'
exp_map['batch_696']='Plus30-Future' # Note this is being rerun!
# Bias correction simulations (split into three 10 year slices)
exp_map['batch_553']='All-Hist'
exp_map['batch_554']='All-Hist'
exp_map['batch_555']='All-Hist'

# HadRM3P Europe runs
exp_map['batch_646']='All-Hist'
exp_map['batch_647']='Plus15-Future'
exp_map['batch_648']='Plus20-Future'
exp_map['batch_709']='All-Hist' # This is the 30 year climatology

# HadRM3P South Asia 50km simulations
exp_map['batch_535']='GHGOnly-Hist'
exp_map['batch_634']='Plus15-Future'
exp_map['batch_635']='Plus20-Future'
exp_map['batch_633']='All-Hist'
exp_map['batch_697']='All-Hist' # This is the 30 year climatology


###############################################################################
#
# Dictionary of standard variable names to match with hadam3p item codes

var_rename={'item3236_daily_maximum':'tasmax',
	'item3236_daily_minimum':'tasmin',
	'item3236_daily_mean':'tas',
	'item3236_monthly_mean':'tas',
	'item5216_daily_mean':'pr',
	'item5216_monthly_mean':'pr',
	'item8234_monthly_mean':'mrros',
	'item8235_monthly_mean':'mrro',
	'item3234_monthly_mean':'hfls',
	'item15201_daily_mean':'ua',
	'item15202_daily_mean':'va',
	'item15201_monthly_mean':'ua',
	'item15202_monthly_mean':'va',
	'item1201_monthly_mean':'rsds',
	'item3245_monthly_mean':'hurs',
	'item3245_daily_mean':'hurs'
}


################################################################################
