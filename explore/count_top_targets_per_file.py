import sys, os, glob, numpy, Queue, csv
from multiprocessing import Pool

sys.path.append(os.path.join(os.path.dirname(os.path.join(os.getcwd(), __file__)), '..'))
from lib import read_vm_file

def count_top_targets_per_file(src, dest):
	print "Processing %s" % src
	data = read_vm_file(src)
	click_counts = {}
	for referrer, target, num_clicks in data:
		if target not in click_counts:
			click_counts[target] = 0
		click_counts[target] += num_clicks

	total_count = float(numpy.sum(click_counts.values()))
	sorted_counts = sorted(click_counts.items(), key=lambda tupl: tupl[1], reverse=True)
	cum_count = 0
	with open(dest, 'w') as destf:
		writer = csv.writer(destf, delimiter="\t")
		for target, count in sorted_counts:
			cum_count += count
			writer.writerow([target, count, (total_count - cum_count) / total_count])

def worker(params):
	return count_top_targets_per_file(*params)

def run_in_parallel(files, dest_dir):
	params = []
	for f in files:
		remainder, filename = os.path.split(f)
		remainder, week = os.path.split(remainder)
		remainder, year = os.path.split(remainder)
		_, cat = os.path.split(remainder)
		destf = os.path.join(dest_dir, cat, year, week, filename)
		if not os.path.exists(os.path.dirname(destf)):
			os.makedirs(os.path.dirname(destf))
		params.append((f, destf))
	p = Pool(processes=10)
	results = p.map(worker, params)

def main():
	run_in_parallel(
		glob.glob(os.path.join(os.getenv("TD"), "vm", "level2-domain", "month", "over-time-samples", "*", "*", "*", "*.txt")),
		os.path.join(os.getenv("TR"), "top-targets", "month", "over-time-samples")
    )

if __name__ == "__main__":
	main()
