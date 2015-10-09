import sys, os, csv, glob, random
from multiprocessing import Pool

from sample import sample_vm_data
from lib import read_vm_file

def read_files(files):
	print "Reading files."
	p = Pool(processes=16)
	results = p.map(read_vm_file, files)
	clicks = {}
	for vm in results:
		for referrer, target, click_count in vm:
			if (referrer, target) not in clicks:
				clicks[(referrer, target)] = 0
			clicks[(referrer, target)] += click_count
	data = []
	for referrer, target in clicks:
		data.append((referrer, target, clicks[(referrer, target)]))
	return data

def create_samples(data, dest_dir, num_clicks_to_sample, num_samples):
	for i in range(num_samples):
		print "Creating sample %d for %s." % (i, dest_dir)
		sample_vm_data(data, os.path.join(dest_dir, "sample%d.txt" % i), num_clicks_to_sample)
	
def worker(params):
	return create_samples(*params)
	
def main():
	params = []
	for cat in ['search', 'social', 'email']:
		print "Reading data for", cat
		files = glob.glob(os.path.join(os.getenv("TD"), "vm", "level2-domain", "month", "categories", cat, "*", "*", "*.txt"))
		vm = read_files(files)
		dest = os.path.join(os.getenv("TD"), "vm", "level2-domain", "month", "samples", cat)
		if not os.path.exists(dest):
			os.makedirs(dest)
		params.append((vm, dest, 50000, 5))

	p = Pool(processes=3)
	p.map(worker, params)

if __name__ == "__main__":
	main()
