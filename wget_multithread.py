# Script to do monthly means then ensemble average of HAPPI data
import os,glob,tempfile,shutil
import multiprocessing,time

if __name__=='__main__':

	#fwget = './wget_dailyprecip_hist.sh'
	fwget = 'wget-dailyprecip_part2.sh'
	numthreads = 6
	# Create pool of processes to process runs in parallel. 
	pool = multiprocessing.Pool(processes=numthreads)

	for line in open(fwget,'r'):
		if line.strip() != '' and line[0]!='#':
			pool.apply_async(os.system,(line.strip(),))

	# close the pool and make sure the processing has finished
	pool.close()
	pool.join()
