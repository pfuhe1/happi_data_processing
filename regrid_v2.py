import glob,os
#base_path = '/export/silurian/array-01/pu17449/'
#outpath = os.path.join(base_path,'processed_data_clim_2deg')
#inpath = os.path.join(base_path,'processed_data_clim',)

#for expt in ['historical','1pt5degC','2pt0degC','1pt5degC_OS']:
#for expt in ['']:
for expt in ['historical','slice15','slice20']:

#	outpath = '/export/silurian/array-01/pu17449/CESM_low_warming/seas_data_regrid/'+expt
#	inpath = '/export/silurian/array-01/pu17449/CESM_low_warming/seas_data_CESM/'+expt

#	inpath = '/export/silurian/array-01/pu17449/seas_data_20170731/'
#	outpath = '/export/silurian/array-01/pu17449/seas_data_regrid/'

	inpath = '/export/silurian/array-01/pu17449/CMIP5_slices/subset_daily/pr/'+expt
	outpath = '/export/silurian/array-01/pu17449/CMIP5_slices/subset_daily_regrid/pr/'+expt

#	inpath = '/export/miocene/array-01/pu17449/CMIP5_patterns'
#	outpath = '/export/miocene/array-01/pu17449/CMIP5_patterns_regrid'

	if not os.path.exists(outpath):
		os.makedirs(outpath)
	print 'INPATH:',inpath
	# Loop over files:
	for fin in glob.glob(inpath+'/*.nc'):
		fname = os.path.basename(fin)
		var = fname.split('_')[1]
#		var = fname.split('.')[1]
		fout = os.path.join(outpath,fname)
		#cmd = 'cdo remapcon2,r144x96 '+fin +' processed_data_clim_2deg/'+fname
		if not os.path.exists(fout):
			# optionally select variable (needed if multiple variables are in the input file)
			#cmd = 'cdo remapcon2,/export/silurian/array-01/pu17449/mygrid -selvar,'+var+' '+fin +' '+fout
			cmd = 'cdo remapcon2,/export/silurian/array-01/pu17449/mygrid '+fin +' '+fout
			print cmd
			os.system('time '+cmd)
