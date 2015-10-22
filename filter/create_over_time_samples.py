import sys, os, csv, glob, random
from multiprocessing import Pool

from sample import sample_vm

def worker(params):
	return sample_vm(*params)

def run_in_parallel(src_files, dest_dir, num_to_sample, num_processes):
	params = []
	for f in src_files:
		remainder, filename = os.path.split(f)
		remainder, month = os.path.split(remainder)
		remainder, year = os.path.split(remainder)
		_, category = os.path.split(remainder)

		dest = os.path.join(dest_dir, category, year, month, filename)
		if not os.path.exists(os.path.dirname(dest)):
			os.makedirs(os.path.dirname(dest))
		params.append((f, dest, num_to_sample))
			
	p = Pool(processes=num_processes)
	p.map(worker, params)
	
def main():
	files = glob.glob(os.path.join(os.getenv("TD"), "vm", "news", "full-domain", "month", "categories", "*", "*", "*", "*.txt"))
	dest = os.path.join(os.getenv("TD"), "vm", "news", "full-domain", "month", "over-time-samples")
	run_in_parallel(files, dest, 80000, 16)

def test():
	src = os.path.join(os.getenv("TD"), "vm", "test", "level3-domain", "month", "2007", "05", "2007-05.txt")
	dest = os.path.join(os.getenv("TD"), "vm", "test", "output", "2007-05-sample.txt")
	if not os.path.exists(os.path.dirname(dest)):
		os.makedirs(os.path.dirname(dest))
	worker((src, dest, 100, 2))

if __name__ == "__main__":
	main()
