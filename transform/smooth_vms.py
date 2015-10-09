import sys, os, csv, glob, random
from multiprocessing import Pool

sys.path.append(os.path.join(os.path.dirname(os.path.join(os.getcwd(), __file__)), '..'))
from lib import read_vm_file

def smooth_vm(files, dest):
	print "Processing %s" % dest
	vms = [read_vm_file(f) for f in files]
	data = {}
	for vm in vms:
		for referrer, target, num_clicks in vm:
			if referrer not in data:
				data[referrer] = {}
			if target not in data[referrer]:
				data[referrer][target] = 0
			data[referrer][target] += num_clicks

	with open(dest, 'w') as destf:
		writer = csv.writer(destf, delimiter="\t")
		for referrer in data.keys():
			for target in data[referrer].keys():
				writer.writerow([referrer, target, data[referrer][target]])

def worker(params):
	return smooth_vm(*params)

def run_in_parallel(src_files, dest_dir, smooth_window):
	files_by_cat = {}
	for f in src_files:
		remainder, filename = os.path.split(f)
		remainder, month = os.path.split(remainder)
		remainder, year = os.path.split(remainder)
		_, cat = os.path.split(remainder)

		if cat not in files_by_cat:
			files_by_cat[cat] = []
		files_by_cat[cat].append(f)

	params = []
	for cat in files_by_cat.keys():
		for i in range(len(files_by_cat[cat])):
			if i + smooth_window > len(files_by_cat[cat]):
				break
			else:
				current_window = files_by_cat[cat][i:i+smooth_window]
				middle_filepath = current_window[smooth_window / 2]
				remainder, filename = os.path.split(middle_filepath)
				remainder, month = os.path.split(remainder)
				_, year = os.path.split(remainder)
				dest = os.path.join(dest_dir, cat, year, month, filename)
				if not os.path.exists(os.path.dirname(dest)):
					os.makedirs(os.path.dirname(dest))
				params.append((current_window, dest))

	p = Pool(processes=10)
	results = p.map(worker, params)
	
def main():
	files = glob.glob(os.path.join(os.getenv("TD"), "vm", "news-only", "full-domain", "month", "over-time-samples", "categories", "*", "*", "*", "*.txt"))
	dest = os.path.join(os.getenv("TD"), "vm", "news-only", "full-domain", "month", "smooth", "categories")
	run_in_parallel(files, dest, 3)

def test():
	files = glob.glob(os.path.join(os.getenv("TD"), "vm", "test", "level2-domain", "month", "smooth-test", "2007", "*", "*.txt"))
	dest = os.path.join(os.getenv("TD"), "vm", "test", "output", "2007-smooth")
	if not os.path.exists(os.path.dirname(dest)):
		os.makedirs(os.path.dirname(dest))
	run_in_parallel(files, dest, 3)

if __name__ == "__main__":
	#test()
	main()
