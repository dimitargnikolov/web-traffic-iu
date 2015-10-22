import sys, os, csv, glob, random, numpy
from multiprocessing import Pool

sys.path.append(os.getenv("TC"))
from lib import read_vm_file

def index_vms(files):
	print "Loading index."
	vms = {}
	for filepath in files:
		data = read_vm_file(filepath)
		for referrer, target, num_clicks in data:
			if referrer not in vms:
				vms[referrer] = {}
			if target not in vms[referrer]:
				vms[referrer][target] = 0
			vms[referrer][target] += num_clicks

	index = []
	total = 0
	for referrer in vms:
		for target in vms[referrer]:
			total += vms[referrer][target]
			index.append((referrer, target, total))

	return index

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

def sample_vm(index, num_clicks_to_sample, catname):
	print "Processing %s %d" % (catname, num_clicks_to_sample)
	sample = {}
	count = 0
	while count < num_clicks_to_sample:
		referrer, target = choose_random_click(index)
		if referrer not in sample:
			sample[referrer] = {}
		if target not in sample[referrer]:
			sample[referrer][target] = 0
		sample[referrer][target] += 1
		count += 1
	
	vm = []
	for referrer in sample:
		for target in sample[referrer]:
			vm.append((referrer, target, sample[referrer][target]))

	return vm

def worker(params):
	return sample_vm(*params)

def run_in_parallel(src_files, dest_dir, sample_sizes, num_processes):
	files_by_cat = {}
	for f in src_files:
		remainder, filename = os.path.split(f)
		remainder, month = os.path.split(remainder)
		remainder, year = os.path.split(remainder)
		_, category = os.path.split(remainder)
		if category not in files_by_cat:
			files_by_cat[category] = []
		files_by_cat[category].append(f)

	# load indices in parallel
	cats = sorted(files_by_cat.keys())
	p = Pool(processes=3)
	indices = p.map(index_vms, [files_by_cat[cat] for cat in cats])
	
	# in parallel, sample each sample size for each category
	params = []
	for i in range(len(cats)):
		for sample_size in sample_sizes:
			params.append((indices[i], sample_size, cats[i]))
	params = sorted(params, key=lambda p: p[1])
	print len(params)
	p = Pool(processes=num_processes)
	vms = p.map(worker, params)

	# save the sampled vms
	for i in range(len(params)):
		_, sample_size, cat = params[i]
		vm = vms[i]
		dest = os.path.join(dest_dir, cat, "%s-%d-clicks.txt" % (cat, sample_size))
		if not os.path.exists(os.path.dirname(dest)):
			os.makedirs(os.path.dirname(dest))
		with open(dest, 'w') as destf:
			writer = csv.writer(destf, delimiter="\t")
			for row in vm:
				writer.writerow(row)
	
def main():
	sample_sizes = []
	#for n in numpy.arange(0.01, 6.5, 0.2): # news
	for n in numpy.arange(0.01, 8.21, 0.2): # all
		sample_sizes.append(int(10**n))

	files = glob.glob(os.path.join(os.getenv("TD"), "vm", "level2-domain", "month", "categories", "*", "*", "*", "*.txt"))
	dest = os.path.join(os.getenv("TD"), "vm", "level2-domain", "month", "entropy-vs-volume")

	run_in_parallel(files, dest, sample_sizes, 10)

def test():
	files = glob.glob(os.path.join(os.getenv("TD"), "vm", "test", "level2-domain", "month", "categories", "*", "*", "*", "*"))
	dest_dir = os.path.join(os.getenv("TD"), "vm", "test", "output", "multiple-samples")
	if not os.path.exists(dest_dir):
		os.makedirs(dest_dir)
	run_in_parallel(files, dest_dir, [10, 100])

if __name__ == "__main__":
	main()
