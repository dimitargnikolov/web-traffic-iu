import sys, os, glob, numpy
from multiprocessing import Pool

sys.path.append(os.path.join(os.path.dirname(os.path.join(os.getcwd(), __file__)), '..'))
from lib import read_vm_file

def count_requests(src):
	total = 0
	with open(src, 'r') as f:
		total = len(f.readlines())
	return total

def run_in_parallel(files):
	p = Pool(processes=16)
	results = p.map(count_requests, files)
	return numpy.sum(results)
	
def main():
	print run_in_parallel(glob.glob(os.path.join(os.getenv("TD"), "valid-requests", "*", "*", "*.click")))

if __name__ == "__main__":
	main()
