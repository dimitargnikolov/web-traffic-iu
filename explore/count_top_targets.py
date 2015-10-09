import sys, os, glob, numpy, Queue
from multiprocessing import Pool

sys.path.append(os.path.join(os.path.dirname(os.path.join(os.getcwd(), __file__)), '..'))
from lib import read_vm_file

def show_top_targets(src):
	print "Processing %s" % src
	data = read_vm_file(src)
	click_counts = {}
	for referrer, target, num_clicks in data:
		if target not in click_counts:
			click_counts[target] = 0
		click_counts[target] += num_clicks
	return click_counts

def run_in_parallel(files):
	p = Pool(processes=10)
	results = p.map(show_top_targets, files)

	# combine results
	print "Combining."
	total_click_counts = {}
	for click_counts in results:
		for target in click_counts:
			if target not in total_click_counts:
				total_click_counts[target] = 0
			total_click_counts[target] += click_counts[target]
	total_clicks = numpy.sum(total_click_counts.values())

	# sort the results
	sorted_click_counts = sorted(
		[(target, total_click_counts[target]) for target in total_click_counts],
		key=lambda item: item[1], reverse=True)

	cum_clicks = 0
	for target, clicks in sorted_click_counts:
		cum_clicks += clicks
		percentile = float(cum_clicks) / total_clicks
		if percentile <= 20:
			print target, clicks, percentile

def main():
	run_in_parallel(glob.glob(os.path.join(os.getenv("TD"), "vm", "level2-domain", "week", "categories", "social", "*", "*", "*.txt")))

if __name__ == "__main__":
	#test()
	main()
