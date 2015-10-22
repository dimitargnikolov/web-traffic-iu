import sys, os, numpy, csv, glob, re
from multiprocessing import Pool

sys.path.append(os.getenv("TC"))
from lib import read_vm_file

def combine_files(files, destfile):
	print "Processing %s" % destfile
	clicks = {}
	for f in files:
		data = read_vm_file(f)
		for referrer, target, num_clicks in data:
			if referrer not in clicks:
				clicks[referrer] = {}
			if target not in clicks[referrer]:
				clicks[referrer][target] = 0
			clicks[referrer][target] += num_clicks
	
	with open(destfile, 'w') as destf:
		writer = csv.writer(destf, delimiter="\t")
		for referrer in clicks:
			for target in clicks[referrer]:
				writer.writerow([referrer, target, clicks[referrer][target]])

def worker(params):
	return combine_files(*params)

def run_in_parallel(files, dest_dir, num_processes):
	file_groups = {}
	for f in files:
		_, filename = os.path.split(f)
		m = re.search(r'(\d+)\-(\d+)\-(\d+)\-(\d+)\-(\d+)', filename)
		if m is not None:
			year, month, day, hour, minute = m.groups()
			if (year, month, day) not in file_groups:
				file_groups[(year, month, day)] = []
			file_groups[(year, month, day)].append(f)
		else:
			print "Skipping", filename

	params = []
	for year, month, day in file_groups:
		destf = os.path.join(dest_dir, year, month, "%s-%s-%s.txt" % (year, month, day))
		if not os.path.exists(os.path.dirname(destf)):
			os.makedirs(os.path.dirname(destf))
		params.append((file_groups[(year, month, day)], destf))
	
	p = Pool(processes=num_processes)
	results = p.map(worker, params)
		
def test1():
	src_dir = os.path.join(os.getenv("TD"), "vm", "full-domain", "hour", "2007", "05")
	dest_dir = os.path.join(os.getenv("TD"), "vm", "test")
	if not os.path.exists(dest_dir):
		os.makedirs(dest_dir)
	combine_files(
		[
			os.path.join(src_dir, "2007-05-19-00-00.txt"), 
			os.path.join(src_dir, "2007-05-19-01-00.txt")
		],
		os.path.join(dest_dir, "2007-05-19.txt")
	)

def test2():
	run_in_parallel(
		glob.glob(os.path.join(os.getenv("TD"), "vm", "full-domain", "hour", "*", "*", "2007-05-*-*-*.txt")),
		os.path.join(os.getenv("TD"), "vm", "test"), 
		2
    )

def main():
	run_in_parallel(
		glob.glob(os.path.join(os.getenv("TD"), "vm", "full-domain", "hour", "*", "*", "*.txt")),
		os.path.join(os.getenv("TD"), "vm", "full-domain", "day"),
		16
	)

if __name__ == "__main__":
	main()
