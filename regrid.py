import glob,os
base_path = '/export/silurian/array-01/pu17449/'
for fin in glob.glob(base_path+'/processed_data_clim/*.nc'):
	fname = os.path.basename(fin)
	fout = os.path.join(base_path,'processed_data_clim_2deg',fname)
	#cmd = 'cdo remapcon2,r144x96 '+fin +' processed_data_clim_2deg/'+fname
	if not os.path.exists(fout):
		cmd = 'cdo remapcon2,'+base_path+'/mygrid '+fin +' '+fout
		print cmd
		os.system(cmd)
