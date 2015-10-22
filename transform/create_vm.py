import sys, os, numpy, csv, glob
from multiprocessing import Pool

sys.path.append(os.path.join(os.getenv("TC"), "filter"))
from validate_requests import parse_dt_from_filename

def create_vm(src, dest):
	print "Processing", src
	clicks = {}
	with open(src, 'r') as f:
		reader = csv.reader(f, delimiter="\t")
		try:
			reader.next() # skip header line
		except StopIteration:
			return # file is empty
		for row in reader:
			timestamp, referrer, target = row
			if referrer not in clicks:
				clicks[referrer] = {}
			if target not in clicks[referrer]:
				clicks[referrer][target] = 0
			clicks[referrer][target] += 1
	
	with open(dest, 'w') as destf:
		writer = csv.writer(destf, delimiter="\t")
		for referrer in clicks:
			for target in clicks[referrer]:
				writer.writerow([referrer, target, clicks[referrer][target]])

def worker(params):
	return create_vm(*params)

def run_in_parallel(files, dest_dir, num_processes):
	params = []
	for f in files:
		remainder, filename = os.path.split(f)
		remainder, month = os.path.split(remainder)
		_, year = os.path.split(remainder)

		dt = parse_dt_from_filename(filename)
		destf = os.path.join(dest_dir, year, month, "%04d-%02d-%02d-%02d-%02d.txt" % (dt.year, dt.month, dt.day, dt.hour, dt.minute))

		if not os.path.exists(os.path.dirname(destf)):
			os.makedirs(os.path.dirname(destf))
		params.append((f, destf))
	
	p = Pool(processes=num_processes)
	results = p.map(worker, params)

def main():
	run_in_parallel(
		glob.glob(os.path.join(os.getenv("TD"), "valid-requests", "*", "*", "*")),
		os.path.join(os.getenv("TD"), "vm", "full-domain", "hour"),
		16
    )
		
def test1():
	dest = os.path.join(os.getenv("TD"), "vm", "test", "2008", "04", "2008-04-29-00-00.txt")
	if not os.path.exists(os.path.dirname(dest)):
		os.makedirs(os.path.dirname(dest))
	create_vm(
		os.path.join(os.getenv("TD"), "valid-requests", "2008", "04", "2008-04-29_19:00:00_+3600.click"), 
		dest
	)

def test2():
	dest_dir = os.path.join(os.getenv("TD"), "vm", "test")
	if not os.path.exists(dest_dir):
		os.makedirs(dest_dir)
	run_in_parallel(
		glob.glob(os.path.join(os.getenv("TD"), "valid-requests", "2007", "05", "2007-05-19_1*")), 
		dest_dir,
		5
	)

if __name__ == "__main__":
	main()
