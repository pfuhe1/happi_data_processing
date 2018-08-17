import glob,os
base_path = '/export/anthropocene/array-01/pu17449/'
for fin in glob.glob(base_path+'/happi_processed/seas_ensmean/*seasclim_ensmean.nc'):
	fname = os.path.basename(fin)
	fout = os.path.join(base_path,'happi_processed_2deg/seas_ensmean',fname)
	print fout
	#cmd = 'cdo remapcon2,r144x96 '+fin +' processed_data_clim_2deg/'+fname
	if not os.path.exists(fout):
		cmd = 'cdo remapcon2,'+base_path+'/mygrid '+fin +' '+fout
		print cmd
		os.system(cmd)
