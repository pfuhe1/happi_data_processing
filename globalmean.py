import numpy as np
# Function to calculate global mean of lat-lon data based on 
# weighted average, weighting by cos of latitude
# Handles the special case of masked data
#
def calc_globalmean(data,lat):
	# Assume data has shape time,lat,lon or just lat,lon
	shp = data.shape
	if len(shp)==3:
		nt,nlat,nlon = shp
	elif len(shp)==2:
		nlat,nlon = shp
		nt = 1
	# Initialise output array
	data_globalmean = np.zeros([nt])
	# calculate weights (convert degrees to radians)
	if len(lat.shape)==1:
		weight1D = np.cos(lat/180.*np.pi)
		# repeat the 1D array across all longitudes
		weight2D = np.repeat(weight1D[:,np.newaxis],nlon,axis=1)
	else:
		weight2D = np.cos(lat/180.*np.pi)
	# the data array is a masked array, so we need to set the same mask for the weights as the data
	if len(shp)==3:
		weighting = np.ma.masked_where(data.mask[0,:],weight2D)
	else:
		weighting = np.ma.masked_where(data.mask,weight2D)
	# the sum of weights is needed for the weighted average
	weightsum = weighting.sum()
	
	# Loop over times and calculated the global average
	for t in range(nt):
		if len(shp)==3:
			data_globalmean[t] = (data[t,:]*weighting).sum()/weightsum
		else: # no time dimension
			data_globalmean[t] = (data[:]*weighting).sum()/weightsum

	# Return the calculated data
	return data_globalmean

