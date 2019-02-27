import gdal
import numpy as np
import os,glob,shutil
import netCDF4

################################################################################
# Set up paths etc. 

dem_file = '/export/anthropocene/array-01/pu17449/MeritDEM/n25e090_dem.tif'
grid_template = '/export/anthropocene/array-01/pu17449/isimip_bc/obs/EWEMBI/processing/EWEMBI_n25e090.BCmask.nc4'

# Netcdf file, created by running command 'cdo gridarea' on grid template file
grid_area = '/home/bridge/pu17449/src/MetSim/testrun/gridarea_cdo.nc'

# output 'domain' file for metsim
outdomain_file = '/home/bridge/pu17449/src/MetSim/testrun/domain_n25e090.nc'

# output elev_band file for FUSE:
elevbands_file = '/home/bridge/pu17449/src/MetSim/testrun/GBM_testrun_elev_bands.nc'

# open grid info
f_grid = netCDF4.Dataset(grid_template)
latvals = f_grid.variables['lat'][:]
lonvals = f_grid.variables['lon'][:]
n_outx = len(latvals)
n_outy = len(lonvals)

# number of DEM points per grid box
stepx = 600
stepy = 600

# Number of elevation bands to use for FUSE (user choice)
nbands = 6

################################################################################
# initialise output arrays:
elev = np.zeros([n_outy,n_outx],dtype=np.int)
mask = np.ones([n_outy,n_outx],dtype=np.int64)
frac = np.ones([n_outy,n_outx])

band_elev = np.zeros([nbands,n_outy,n_outx])
band_frac = np.zeros([nbands,n_outy,n_outx])

# calculate (approx) grid box area
#area_p5deg = (111000/2.)**2.
# calculate weights (convert degrees to radians)
#area1D = area_p5deg * np.cos(latvals/180.*np.pi)
# repeat the 1D array across all longitudes
#area = np.repeat(area1D[:,np.newaxis],n_outx,axis=1)

# Use grid area calculated by cdo 
with netCDF4.Dataset(grid_area,'r') as f_area:
	area = f_area.variables['cell_area']

################################################################################
# Read in data to calculate grid box elevations
f=gdal.Open(dem_file)
rasterband = f.GetRasterBand(1)
dem_arr = rasterband.ReadAsArray()
for x in range(n_outx):
	for y in range(n_outy):
		box = dem_arr[y*stepy:(y+1)*stepy,x*stepx:(x+1)*stepx]
		box.flatten()
		# Mean elevation
		elev[y,x] = box.mean()
		# Now split elevation into bands
		min_elev = box.min()
		max_elev = box.max()
		bands = np.linspace(min_elev,max_elev,num=nbands+1)

		for i in range(nbands):
			subset = box[np.logical_and(box> bands[i],box<bands[i+1])]
			band_elev[i,y,x] = subset.mean()
			if i == nbands-1: # Handle last band separately
				# Hack to make sure fracs sum to 1 
				# (otherwise there is a small rounding error) 
				band_frac[i,y,x] = 1- band_frac[:-1,y,x].sum()
			else:
				band_frac[i,y,x] = len(subset)/(stepx*stepy*1.)
		print('debug area_sum',band_frac[:,y,x].sum())
		
	
#print('debug, exiting without writing files')
#sys.exit(1)
	
###############################################################################
# Write output file for modsim: 

# Follows format of variables needed in 'domain' file for metsim e.g. '/home/bridge/pu17449/src/MetSim/metsim/data/domain.nc'
with netCDF4.Dataset(outdomain_file,'w') as f_out_modsim:

	f_out_modsim.createDimension('lat',n_outy)
	f_out_modsim.createVariable('lat',np.float,('lat'))
	f_out_modsim.variables['lat'].standard_name = "latitude"
	f_out_modsim.variables['lat'].long_name = "latitude"
	f_out_modsim.variables['lat'].units = "degrees_north"
	f_out_modsim.variables['lat'].axis = "Y"
	f_out_modsim.variables['lat'][:] = latvals

	f_out_modsim.createDimension('lon',n_outx)
	f_out_modsim.createVariable('lon',np.float,('lon'))
	f_out_modsim.variables['lon'].standard_name = "longitude"
	f_out_modsim.variables['lon'].long_name = "longitude"
	f_out_modsim.variables['lon'].units = "degrees_east"
	f_out_modsim.variables['lon'].axis = "X"
	f_out_modsim.variables['lon'][:] = lonvals

	f_out_modsim.createVariable('elev',np.int32,('lat','lon'),fill_value=-9999)
	#f_out_modsim.variables['elev']._FillValue = -9999
	f_out_modsim.variables['elev'].long_name = 'gridcell_elevation'
	f_out_modsim.variables['elev'].units = 'm'
	f_out_modsim.variables['elev'][:] = elev

	f_out_modsim.createVariable('mask',np.int64,('lat','lon'))
	f_out_modsim.variables['mask'].long_name = 'domain_mask'
	f_out_modsim.variables['mask'].comment = '0 indicates cell is not active'
	f_out_modsim.variables['mask'][:] = mask

	f_out_modsim.createVariable('frac',np.float,('lat','lon'))
	f_out_modsim.variables['frac'].long_name = 'fraction of grid cell that is active'
	f_out_modsim.variables['frac'].units = '1'
	f_out_modsim.variables['frac'][:] = frac

	f_out_modsim.createVariable('area',np.float,('lat','lon'))
	f_out_modsim.variables['area'].standard_name = 'area'
	f_out_modsim.variables['area'].long_name = 'area of grid cell'
	f_out_modsim.variables['area'].units = 'm2'
	f_out_modsim.variables['area'][:] = area



###############################################################################
# Write output file for FUSE elev bands
# Follows example of file from fuse_grid example: cesm1-cam5_elev_bands.nc

with netCDF4.Dataset(elevbands_file,'w') as f_out:
	f_out.createDimension('latitude',n_outy)
	f_out.createVariable('latitude',np.float,('latitude'))
	f_out.variables['latitude'].standard_name = "latitude"
	f_out.variables['latitude'].long_name = "latitude"
	f_out.variables['latitude'].units = "degrees_north"
	f_out.variables['latitude'].axis = "Y"
	f_out.variables['latitude'][:] = latvals

	f_out.createDimension('longitude',n_outx)
	f_out.createVariable('longitude',np.float,('longitude'))
	f_out.variables['longitude'].standard_name = "longitude"
	f_out.variables['longitude'].long_name = "longitude"
	f_out.variables['longitude'].units = "degrees_east"
	f_out.variables['longitude'].axis = "X"
	f_out.variables['longitude'][:] = lonvals

	f_out.createDimension('elevation_band',nbands)
	f_out.createVariable('elevation_band',np.int32,('elevation_band'))
	f_out.variables['elevation_band'].long_name = 'elevation_band'
	f_out.variables['elevation_band'].units = '-'
	f_out.variables['elevation_band'][:] = range(1,nbands+1)

	f_out.createVariable('area_frac',np.float32,('elevation_band', 'latitude', 'longitude'), fill_value=-9999)
	f_out.variables['area_frac'].units = "-" ;
	f_out.variables['area_frac'].long_name = "Fraction of grid cell covered by each elevation band." 
	f_out.variables['area_frac'][:] = band_frac

	f_out.createVariable('mean_elev',np.float32,('elevation_band', 'latitude', 'longitude'),fill_value=-9999)
	f_out.variables['mean_elev'].units = "m asl" ;
	f_out.variables['mean_elev'].long_name = "Mean elevation of elevation band." ;
	f_out.variables['mean_elev'][:] = band_elev

	f_out.createVariable('prec_frac',np.float32,('elevation_band', 'latitude', 'longitude'), fill_value=-9999)
	f_out.variables['prec_frac'].units = "-" ;
	f_out.variables['prec_frac'].long_name = "Fraction of cell precipitation that falls on each elevation band." ;
	f_out.variables['prec_frac'].comment = 'PFU note: prec frac is just area_frac for now, need to implement scaling/lapse rate with elevation'
	f_out.variables['prec_frac'][:]=band_frac
