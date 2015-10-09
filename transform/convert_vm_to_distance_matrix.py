import sys, os, numpy, csv, glob
from multiprocessing import Pool
from scipy.sparse import dok_matrix

sys.path.append(os.getenv("TC"))
from lib import read_vm_file

def convert_to_distance_matrix(src, dest):
	print "Reading vm."
	vm = read_vm_file(src)

	print "Getting data statistics."
	clicks = {}
	hosts = set()
	max_clicks = 0
	min_clicks = 1000
	for referrer, target, num_clicks in vm:
		if referrer not in clicks:
			clicks[referrer] = {}
		if target not in clicks[referrer]:
			clicks[referrer][target] = num_clicks

		if referrer not in hosts:
			hosts.add(referrer)
		if target not in hosts:
			hosts.add(target)

		if num_clicks > max_clicks:
			max_clicks = num_clicks
		if num_clicks < min_clicks:
			min_clicks = num_clicks

	print "Sorting hosts and creating host to index mapping."
	sorted_hosts = sorted(hosts)
	host_indices = {}
	count = 0
	for host in sorted_hosts:
		host_indices[host] = count
		count += 1
	print "Number of hosts:", len(sorted_hosts)

	print "Converting to dok matrix."
	delta = float(max_clicks - min_clicks)
	m = dok_matrix((len(hosts), len(hosts)), dtype=numpy.float32)
	for referrer in clicks:
		for target in clicks[referrer]:
			m[host_indices[referrer], host_indices[target]] = (clicks[referrer][target] - min_clicks) / delta
			

def main():
	src = os.path.join(os.getenv("TD"), "vm", "news-full-unfiltered.txt")
	dest = os.path.join(os.getenv("TD"), "vm", "news-full-distances.txt")
	convert_to_distance_matrix(src, dest)

if __name__ == "__main__":
	main()
