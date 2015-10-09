import sys, os, csv, glob, random

sys.path.append(os.path.join(os.getenv("TC")))
from lib import read_vm_file

def choose_random_click(index):
	r = random.randint(1, index[-1][2])
	begin = 0
	end = len(index) - 1
	middle = (end - begin + 1) / 2
	while True:
		referrer, target, cum_clicks = index[middle]
		if begin == end:
			return referrer, target
		elif r > cum_clicks:
			begin = middle
			middle += (end - begin + 1) / 2
		else:
			end = middle - 1
			middle -= (end - begin + 1) / 2

def sample_vm(src, dest, num_clicks_to_sample):
	return sample_vm_data(read_vm_file(src), dest, num_clicks_to_sample)

def sample_vm_data(data, dest, num_clicks_to_sample):
	print "Processing %s" % dest

	# create an index where to each web path 
	# corresponds the number of clicks that
	# occurred for it and all previous web paths
	index = []
	total_clicks = 0
	for referrer, target, num_clicks in data:
		total_clicks += num_clicks
		index.append((referrer, target, total_clicks))

	# do weighted sampling from the web paths
	sample = {}
	count = 0
	while count <= num_clicks_to_sample:
		referrer, target = choose_random_click(index)
		if referrer not in sample:
			sample[referrer] = {}
		if target not in sample[referrer]:
			sample[referrer][target] = 0
		sample[referrer][target] += 1
		count += 1
	
	# write the results
	with open(dest, 'w') as destf:
		writer = csv.writer(destf, delimiter="\t")
		for referrer in sample.keys():
			for target in sample[referrer].keys():
				writer.writerow([referrer, target, sample[referrer][target]])

if __name__ == "__main__":
	print "This file is not meant to be executed."
