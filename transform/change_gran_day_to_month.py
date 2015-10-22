import sys, os, numpy, csv, glob
from multiprocessing import Pool

from change_gran_hour_to_day import worker

sys.path.append(os.getenv("TC"))
from lib import read_vm_file

def run_in_parallel(files, dest_dir, num_processes):
	file_groups = {}
	for f in files:
		remainder, filename = os.path.split(f)
		remainder, month = os.path.split(remainder)
		_, year = os.path.split(remainder)
		if (year, month) not in file_groups:
			file_groups[(year, month)] = []
		file_groups[(year, month)].append(f)
	
	params = []
	for year, month in file_groups:
		destf = os.path.join(dest_dir, year, month, "%s-%s.txt" % (year, month))
		if not os.path.exists(os.path.dirname(destf)):
			os.makedirs(os.path.dirname(destf))
		params.append((file_groups[(year, month)], destf))
	p = Pool(processes=10)
	results = p.map(worker, params)
		
def test1():
	base_dir = os.path.join(os.getenv("TD"), "vm", "test", "2007", "05")
	worker((
		glob.glob(base_dir,"2007-05-*.txt"),
		os.path.join(base_dir, "2007-05.txt")
	))

def test2():
	run_in_parallel(
		glob.glob(os.path.join(os.getenv("TD"), "vm", "test", "2007", "05", "2007-05-*.txt")),
		os.path.join(os.getenv("TD"), "vm", "test"),
		2
	)

def main():
	run_in_parallel(
		glob.glob(os.path.join(os.getenv("TD"), "vm", "full-domain", "day", "*", "*", "*.txt")),
		os.path.join(os.getenv("TD"), "vm", "full-domain", "month"),
		16
	)

if __name__ == "__main__":
	main()
