import gdal
import numpy as np
import os,glob,shutil
import netCDF4


dem_file = '/export/anthropocene/array-01/pu17449/MeritDEM/n25e090_dem.tif'
grid_template = '/export/anthropocene/array-01/pu17449/isimip_bc/obs/EWEMBI/EWEMBI_n25e090.BCmask.nc4'

grid_area = '/home/bridge/pu17449/src/MetSim/testrun/gridarea_cdo.nc'

# template of variables needed in 'domain' file for metsim
outdomain_template = '/home/bridge/pu17449/src/MetSim/metsim/data/domain.nc'
# output 'domain' file for metsim
outdomain_file = '/home/bridge/pu17449/src/MetSim/testrun/domain_n25e090.nc'

# Copy template file to output file (then we will modify the output file)
#shutil.copy(outdomain_template,outdomain_file)
f_out = netCDF4.Dataset(outdomain_file,'w')

# open grid info
f_grid = netCDF4.Dataset(grid_template)
latvals = f_grid.variables['lat'][:]
lonvals = f_grid.variables['lon'][:]
n_outx = len(latvals)
n_outy = len(lonvals)



# number of DEM points per grid box
stepx = 600
stepy = 600




# initialise output arrays:
elev = np.zeros([n_outy,n_outx],dtype=np.int)
mask = np.ones([n_outy,n_outx],dtype=np.int64)
frac = np.ones([n_outy,n_outx])

band_elev = np.zeros([nbands,n_outy,n_outx])

# calculate (approx) grid box area
#area_p5deg = (111000/2.)**2.
# calculate weights (convert degrees to radians)
#area1D = area_p5deg * np.cos(latvals/180.*np.pi)
# repeat the 1D array across all longitudes
#area = np.repeat(area1D[:,np.newaxis],n_outx,axis=1)

# Use grid area calculated by cdo 
f_area = netCDF.Dataset(grid_area,'r')
area = f_area.varialbes['cell_area']
nbands = 6

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
		min_elev = box.minimum()
		max_elev = box.maximum()
		bands = np.linspace(min_elev,max_elev,num=nbands+1)

		for i in range(nbands):
			subset = box> bands[i] and box<bands[i+1]
			band_elev[i,y,x] = subset.mean()
			band_frac[i,y,x] = len(subset)/(stepx*stepy*1.)
		
		

# Write output file:

f_out.createDimension('lat',n_outy)
f_out.createVariable('lat',np.float,('lat'))
f_out.variables['lat'].standard_name = "latitude"
f_out.variables['lat'].long_name = "latitude"
f_out.variables['lat'].units = "degrees_north"
f_out.variables['lat'].axis = "Y"
f_out.variables['lat'][:] = latvals

f_out.createDimension('lon',n_outx)
f_out.createVariable('lon',np.float,('lon'))
f_out.variables['lon'].standard_name = "longitude"
f_out.variables['lon'].long_name = "longitude"
f_out.variables['lon'].units = "degrees_east"
f_out.variables['lon'].axis = "X"
f_out.variables['lon'][:] = lonvals

f_out.createVariable('elev',np.int32,('lat','lon'),fill_value=-9999)
#f_out.variables['elev']._FillValue = -9999
f_out.variables['elev'].long_name = 'gridcell_elevation'
f_out.variables['elev'].units = 'm'
f_out.variables['elev'][:] = elev

f_out.createVariable('mask',np.int64,('lat','lon'))
f_out.variables['mask'].long_name = 'domain_mask'
f_out.variables['mask'].comment = '0 indicates cell is not active'
f_out.variables['mask'][:] = mask

f_out.createVariable('frac',np.float,('lat','lon'))
f_out.variables['frac'].long_name = 'fraction of grid cell that is active'
f_out.variables['frac'].units = '1'
f_out.variables['frac'][:] = frac

f_out.createVariable('area',np.float,('lat','lon'))
f_out.variables['area'].standard_name = 'area'
f_out.variables['area'].long_name = 'area of grid cell'
f_out.variables['area'].units = 'm2'
f_out.variables['area'][:] = area


