import sys, os, glob, numpy
from multiprocessing import Pool

sys.path.append(os.path.join(os.path.dirname(os.path.join(os.getcwd(), __file__)), '..'))
from lib import read_vm_file

def count_clicks(src):
	print "Processing %s" % src
	total = 0
	data = read_vm_file(src)
	for referrer, target, num_clicks in data:
		total += num_clicks
	return total

def run_in_parallel(files):
	p = Pool(processes=10)
	results = p.map(count_clicks, files)
	return numpy.sum(results)
	
def main():
	#print run_in_parallel(glob.glob(os.path.join(os.getenv("TD"), "vm", "news-only", "full-domain", "month", "filtered", "*", "*", "*.txt")))
	print run_in_parallel(glob.glob(os.path.join(os.getenv("TD"), "vm", "level3-domain", "month", "unfiltered", "*", "*", "*.txt")))

def test():
	print count_clicks(os.path.join(os.getenv("TD"), "vm", "test", "level3-domain", "month", "2007", "05", "2007-05.txt"))

if __name__ == "__main__":
	#test()
	main()
