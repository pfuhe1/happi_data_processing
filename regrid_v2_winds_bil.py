import glob,os
base_path = '/export/anthropocene/array-01/pu17449'
gridspec = os.path.join(base_path,'mygrid')
outpath = os.path.join(base_path,'happi_processed_2deg')
inpath = os.path.join(base_path,'happi_processed',)

inpattern = os.path.join(inpath,'*.?a.*_seasclim_ensmean.nc')

if not os.path.exists(outpath):
	os.makedirs(outpath)

print 'INPATH:',inpath
# Loop over files:
for fin in glob.glob(inpattern):
	fname = os.path.basename(fin)
	var = fname.split('_')[1]
#		var = fname.split('.')[1]
	fout = os.path.join(outpath,fname)
	#cmd = 'cdo remapcon2,r144x96 '+fin +' processed_data_clim_2deg/'+fname
	if not os.path.exists(fout):
		# optionally select variable (needed if multiple variables are in the input file)
		#cmd = 'cdo remapcon2,/export/silurian/array-01/pu17449/mygrid -selvar,'+var+' '+fin +' '+fout
		cmd = 'cdo remapbil,'+gridspec+' '+fin +' '+fout
		print cmd
		os.system('time '+cmd)
