import os,sys,glob


# Use same files as for tas
cdo_cmd_tas = 'cdo cat '+\
'/export/anthropocene/array-01/pu17449/happi_data_extra/HadAM3P-EU25/All-Hist/est2/v1-0/day/atmos/tas/runqclj/tas_Aday_HadAM3P_All-Hist_est2_v1-0_runqclj_1986-01_1986-12.nc '+\
'/export/anthropocene/array-01/pu17449/happi_data_extra/HadAM3P-EU25/All-Hist/est2/v1-0/day/atmos/tas/runqcxi/tas_Aday_HadAM3P_All-Hist_est2_v1-0_runqcxi_1987-01_1987-12.nc '+\
'/export/anthropocene/array-01/pu17449/happi_data_extra/HadAM3P-EU25/All-Hist/est2/v1-0/day/atmos/tas/runqd1z/tas_Aday_HadAM3P_All-Hist_est2_v1-0_runqd1z_1988-01_1988-12.nc '+\
'/export/anthropocene/array-01/pu17449/happi_data_extra/HadAM3P-EU25/All-Hist/est2/v1-0/day/atmos/tas/runqdgr/tas_Aday_HadAM3P_All-Hist_est2_v1-0_runqdgr_1989-01_1989-12.nc '+\
'/export/anthropocene/array-01/pu17449/happi_data_extra/HadAM3P-EU25/All-Hist/est2/v1-0/day/atmos/tas/runqdnp/tas_Aday_HadAM3P_All-Hist_est2_v1-0_runqdnp_1990-01_1990-12.nc '+\
'/export/anthropocene/array-01/pu17449/happi_data_extra/HadAM3P-EU25/All-Hist/est2/v1-0/day/atmos/tas/runqdxc/tas_Aday_HadAM3P_All-Hist_est2_v1-0_runqdxc_1991-01_1991-12.nc '+\
'/export/anthropocene/array-01/pu17449/happi_data_extra/HadAM3P-EU25/All-Hist/est2/v1-0/day/atmos/tas/runqe6f/tas_Aday_HadAM3P_All-Hist_est2_v1-0_runqe6f_1992-01_1992-12.nc '+\
'/export/anthropocene/array-01/pu17449/happi_data_extra/HadAM3P-EU25/All-Hist/est2/v1-0/day/atmos/tas/runqeek/tas_Aday_HadAM3P_All-Hist_est2_v1-0_runqeek_1993-01_1993-12.nc '+\
'/export/anthropocene/array-01/pu17449/happi_data_extra/HadAM3P-EU25/All-Hist/est2/v1-0/day/atmos/tas/runqeow/tas_Aday_HadAM3P_All-Hist_est2_v1-0_runqeow_1994-01_1994-12.nc '+\
'/export/anthropocene/array-01/pu17449/happi_data_extra/HadAM3P-EU25/All-Hist/est2/v1-0/day/atmos/tas/runqezn/tas_Aday_HadAM3P_All-Hist_est2_v1-0_runqezn_1995-01_1995-12.nc '+\
'/export/anthropocene/array-01/pu17449/happi_data_extra/HadAM3P-EU25/All-Hist/est2/v1-0/day/atmos/tas/runqf3j/tas_Aday_HadAM3P_All-Hist_est2_v1-0_runqf3j_1996-01_1996-12.nc '+\
'/export/anthropocene/array-01/pu17449/happi_data_extra/HadAM3P-EU25/All-Hist/est2/v1-0/day/atmos/tas/runqfb8/tas_Aday_HadAM3P_All-Hist_est2_v1-0_runqfb8_1997-01_1997-12.nc '+\
'/export/anthropocene/array-01/pu17449/happi_data_extra/HadAM3P-EU25/All-Hist/est2/v1-0/day/atmos/tas/runqfpe/tas_Aday_HadAM3P_All-Hist_est2_v1-0_runqfpe_1998-01_1998-12.nc '+\
'/export/anthropocene/array-01/pu17449/happi_data_extra/HadAM3P-EU25/All-Hist/est2/v1-0/day/atmos/tas/runqfwr/tas_Aday_HadAM3P_All-Hist_est2_v1-0_runqfwr_1999-01_1999-12.nc '+\
'/export/anthropocene/array-01/pu17449/happi_data_extra/HadAM3P-EU25/All-Hist/est2/v1-0/day/atmos/tas/runqg1z/tas_Aday_HadAM3P_All-Hist_est2_v1-0_runqg1z_2000-01_2000-12.nc '+\
'/export/anthropocene/array-01/pu17449/happi_data_extra/HadAM3P-EU25/All-Hist/est2/v1-0/day/atmos/tas/runqgc5/tas_Aday_HadAM3P_All-Hist_est2_v1-0_runqgc5_2001-01_2001-12.nc '+\
'/export/anthropocene/array-01/pu17449/happi_data_extra/HadAM3P-EU25/All-Hist/est2/v1-0/day/atmos/tas/runqgnp/tas_Aday_HadAM3P_All-Hist_est2_v1-0_runqgnp_2002-01_2002-12.nc '+\
'/export/anthropocene/array-01/pu17449/happi_data_extra/HadAM3P-EU25/All-Hist/est2/v1-0/day/atmos/tas/runqgtb/tas_Aday_HadAM3P_All-Hist_est2_v1-0_runqgtb_2003-01_2003-12.nc '+\
'/export/anthropocene/array-01/pu17449/happi_data_extra/HadAM3P-EU25/All-Hist/est2/v1-0/day/atmos/tas/runqh28/tas_Aday_HadAM3P_All-Hist_est2_v1-0_runqh28_2004-01_2004-12.nc '+\
'/export/anthropocene/array-01/pu17449/happi_data_extra/HadAM3P-EU25/All-Hist/est2/v1-0/day/atmos/tas/runqhgc/tas_Aday_HadAM3P_All-Hist_est2_v1-0_runqhgc_2005-01_2005-12.nc '+\
'/export/anthropocene/array-01/pu17449/happi_data_extra/HadAM3P-EU25/All-Hist/est2/v1-0/day/atmos/tas/runqhmy/tas_Aday_HadAM3P_All-Hist_est2_v1-0_runqhmy_2006-01_2006-12.nc '+\
'/export/anthropocene/array-01/pu17449/happi_data_extra/HadAM3P-EU25/All-Hist/est2/v1-0/day/atmos/tas/runqhxf/tas_Aday_HadAM3P_All-Hist_est2_v1-0_runqhxf_2007-01_2007-12.nc '+\
'/export/anthropocene/array-01/pu17449/happi_data_extra/HadAM3P-EU25/All-Hist/est2/v1-0/day/atmos/tas/runqi37/tas_Aday_HadAM3P_All-Hist_est2_v1-0_runqi37_2008-01_2008-12.nc '+\
'/export/anthropocene/array-01/pu17449/happi_data_extra/HadAM3P-EU25/All-Hist/est2/v1-0/day/atmos/tas/runqijh/tas_Aday_HadAM3P_All-Hist_est2_v1-0_runqijh_2009-01_2009-12.nc '+\
'/export/anthropocene/array-01/pu17449/happi_data_extra/HadAM3P-EU25/All-Hist/est2/v1-0/day/atmos/tas/runqioq/tas_Aday_HadAM3P_All-Hist_est2_v1-0_runqioq_2010-01_2010-12.nc '+\
'/export/anthropocene/array-01/pu17449/happi_data_extra/HadAM3P-EU25/All-Hist/est2/v1-0/day/atmos/tas/runqisp/tas_Aday_HadAM3P_All-Hist_est2_v1-0_runqisp_2011-01_2011-12.nc '+\
'/export/anthropocene/array-01/pu17449/happi_data_extra/HadAM3P-EU25/All-Hist/est2/v1-0/day/atmos/tas/runqj21/tas_Aday_HadAM3P_All-Hist_est2_v1-0_runqj21_2012-01_2012-12.nc '+\
'/export/anthropocene/array-01/pu17449/happi_data_extra/HadAM3P-EU25/All-Hist/est2/v1-0/day/atmos/tas/runqjjl/tas_Aday_HadAM3P_All-Hist_est2_v1-0_runqjjl_2013-01_2013-12.nc '+\
'/export/anthropocene/array-01/pu17449/happi_data_extra/HadAM3P-EU25/All-Hist/est2/v1-0/day/atmos/tas/runcat1/tas_Aday_HadAM3P_All-Hist_est2_v1-0_runcat1_2017-01_2017-12.nc'

cdo_cmd_tasmax = 'cdo cat '+\
'/export/anthropocene/array-01/pu17449/happi_data_extra/HadAM3P-EU25/All-Hist/est2/v1-0/day/atmos/tasmax/runqclj/tasmax_Aday_HadAM3P_All-Hist_est2_v1-0_runqclj_1986-01_1986-12.nc '+\
'/export/anthropocene/array-01/pu17449/happi_data_extra/HadAM3P-EU25/All-Hist/est2/v1-0/day/atmos/tasmax/runqcxi/tasmax_Aday_HadAM3P_All-Hist_est2_v1-0_runqcxi_1987-01_1987-12.nc '+\
'/export/anthropocene/array-01/pu17449/happi_data_extra/HadAM3P-EU25/All-Hist/est2/v1-0/day/atmos/tasmax/runqd1z/tasmax_Aday_HadAM3P_All-Hist_est2_v1-0_runqd1z_1988-01_1988-12.nc '+\
'/export/anthropocene/array-01/pu17449/happi_data_extra/HadAM3P-EU25/All-Hist/est2/v1-0/day/atmos/tasmax/runqdgr/tasmax_Aday_HadAM3P_All-Hist_est2_v1-0_runqdgr_1989-01_1989-12.nc '+\
'/export/anthropocene/array-01/pu17449/happi_data_extra/HadAM3P-EU25/All-Hist/est2/v1-0/day/atmos/tasmax/runqdnp/tasmax_Aday_HadAM3P_All-Hist_est2_v1-0_runqdnp_1990-01_1990-12.nc '+\
'/export/anthropocene/array-01/pu17449/happi_data_extra/HadAM3P-EU25/All-Hist/est2/v1-0/day/atmos/tasmax/runqdxc/tasmax_Aday_HadAM3P_All-Hist_est2_v1-0_runqdxc_1991-01_1991-12.nc '+\
'/export/anthropocene/array-01/pu17449/happi_data_extra/HadAM3P-EU25/All-Hist/est2/v1-0/day/atmos/tasmax/runqe6f/tasmax_Aday_HadAM3P_All-Hist_est2_v1-0_runqe6f_1992-01_1992-12.nc '+\
'/export/anthropocene/array-01/pu17449/happi_data_extra/HadAM3P-EU25/All-Hist/est2/v1-0/day/atmos/tasmax/runqeek/tasmax_Aday_HadAM3P_All-Hist_est2_v1-0_runqeek_1993-01_1993-12.nc '+\
'/export/anthropocene/array-01/pu17449/happi_data_extra/HadAM3P-EU25/All-Hist/est2/v1-0/day/atmos/tasmax/runqeow/tasmax_Aday_HadAM3P_All-Hist_est2_v1-0_runqeow_1994-01_1994-12.nc '+\
'/export/anthropocene/array-01/pu17449/happi_data_extra/HadAM3P-EU25/All-Hist/est2/v1-0/day/atmos/tasmax/runqezn/tasmax_Aday_HadAM3P_All-Hist_est2_v1-0_runqezn_1995-01_1995-12.nc '+\
'/export/anthropocene/array-01/pu17449/happi_data_extra/HadAM3P-EU25/All-Hist/est2/v1-0/day/atmos/tasmax/runqf3j/tasmax_Aday_HadAM3P_All-Hist_est2_v1-0_runqf3j_1996-01_1996-12.nc '+\
'/export/anthropocene/array-01/pu17449/happi_data_extra/HadAM3P-EU25/All-Hist/est2/v1-0/day/atmos/tasmax/runqfb8/tasmax_Aday_HadAM3P_All-Hist_est2_v1-0_runqfb8_1997-01_1997-12.nc '+\
'/export/anthropocene/array-01/pu17449/happi_data_extra/HadAM3P-EU25/All-Hist/est2/v1-0/day/atmos/tasmax/runqfpe/tasmax_Aday_HadAM3P_All-Hist_est2_v1-0_runqfpe_1998-01_1998-12.nc '+\
'/export/anthropocene/array-01/pu17449/happi_data_extra/HadAM3P-EU25/All-Hist/est2/v1-0/day/atmos/tasmax/runqfwr/tasmax_Aday_HadAM3P_All-Hist_est2_v1-0_runqfwr_1999-01_1999-12.nc '+\
'/export/anthropocene/array-01/pu17449/happi_data_extra/HadAM3P-EU25/All-Hist/est2/v1-0/day/atmos/tasmax/runqg1z/tasmax_Aday_HadAM3P_All-Hist_est2_v1-0_runqg1z_2000-01_2000-12.nc '+\
'/export/anthropocene/array-01/pu17449/happi_data_extra/HadAM3P-EU25/All-Hist/est2/v1-0/day/atmos/tasmax/runqgc5/tasmax_Aday_HadAM3P_All-Hist_est2_v1-0_runqgc5_2001-01_2001-12.nc '+\
'/export/anthropocene/array-01/pu17449/happi_data_extra/HadAM3P-EU25/All-Hist/est2/v1-0/day/atmos/tasmax/runqgnp/tasmax_Aday_HadAM3P_All-Hist_est2_v1-0_runqgnp_2002-01_2002-12.nc '+\
'/export/anthropocene/array-01/pu17449/happi_data_extra/HadAM3P-EU25/All-Hist/est2/v1-0/day/atmos/tasmax/runqgtb/tasmax_Aday_HadAM3P_All-Hist_est2_v1-0_runqgtb_2003-01_2003-12.nc '+\
'/export/anthropocene/array-01/pu17449/happi_data_extra/HadAM3P-EU25/All-Hist/est2/v1-0/day/atmos/tasmax/runqh28/tasmax_Aday_HadAM3P_All-Hist_est2_v1-0_runqh28_2004-01_2004-12.nc '+\
'/export/anthropocene/array-01/pu17449/happi_data_extra/HadAM3P-EU25/All-Hist/est2/v1-0/day/atmos/tasmax/runqhgc/tasmax_Aday_HadAM3P_All-Hist_est2_v1-0_runqhgc_2005-01_2005-12.nc '+\
'/export/anthropocene/array-01/pu17449/happi_data_extra/HadAM3P-EU25/All-Hist/est2/v1-0/day/atmos/tasmax/runqhmy/tasmax_Aday_HadAM3P_All-Hist_est2_v1-0_runqhmy_2006-01_2006-12.nc '+\
'/export/anthropocene/array-01/pu17449/happi_data_extra/HadAM3P-EU25/All-Hist/est2/v1-0/day/atmos/tasmax/runqhxf/tasmax_Aday_HadAM3P_All-Hist_est2_v1-0_runqhxf_2007-01_2007-12.nc '+\
'/export/anthropocene/array-01/pu17449/happi_data_extra/HadAM3P-EU25/All-Hist/est2/v1-0/day/atmos/tasmax/runqi37/tasmax_Aday_HadAM3P_All-Hist_est2_v1-0_runqi37_2008-01_2008-12.nc '+\
'/export/anthropocene/array-01/pu17449/happi_data_extra/HadAM3P-EU25/All-Hist/est2/v1-0/day/atmos/tasmax/runqijh/tasmax_Aday_HadAM3P_All-Hist_est2_v1-0_runqijh_2009-01_2009-12.nc '+\
'/export/anthropocene/array-01/pu17449/happi_data_extra/HadAM3P-EU25/All-Hist/est2/v1-0/day/atmos/tasmax/runqioq/tasmax_Aday_HadAM3P_All-Hist_est2_v1-0_runqioq_2010-01_2010-12.nc '+\
'/export/anthropocene/array-01/pu17449/happi_data_extra/HadAM3P-EU25/All-Hist/est2/v1-0/day/atmos/tasmax/runqisp/tasmax_Aday_HadAM3P_All-Hist_est2_v1-0_runqisp_2011-01_2011-12.nc '+\
'/export/anthropocene/array-01/pu17449/happi_data_extra/HadAM3P-EU25/All-Hist/est2/v1-0/day/atmos/tasmax/runqj21/tasmax_Aday_HadAM3P_All-Hist_est2_v1-0_runqj21_2012-01_2012-12.nc '+\
'/export/anthropocene/array-01/pu17449/happi_data_extra/HadAM3P-EU25/All-Hist/est2/v1-0/day/atmos/tasmax/runqjjl/tasmax_Aday_HadAM3P_All-Hist_est2_v1-0_runqjjl_2013-01_2013-12.nc '+\
'/export/anthropocene/array-01/pu17449/happi_data_extra/HadAM3P-EU25/All-Hist/est2/v1-0/day/atmos/tasmax/runcat1/tasmax_Aday_HadAM3P_All-Hist_est2_v1-0_runcat1_2017-01_2017-12.nc'

fout = cdo_cmd_tasmax.split()[-1]

if not os.path.exists(os.path.dirname(fout)):
	os.makedirs(os.path.dirname(fout))
#cmd1 = 'cdo cat '+f1+' '+f2+' tmp.nc'
#print cmd1
#os.system(cmd1)
print cdo_cmd_tasmax
os.system(cdo_cmd_tasmax)
