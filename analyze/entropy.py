import sys, os, numpy, random

sys.path.append(os.getenv("TC"))
from lib import read_vm_file

def read_counts(src):
	print "Processing %s" % src
	data = read_vm_file(src)
	counts = []
	for _, _, num_clicks in data:
		counts.append(num_clicks)
	return counts

def read_counts_as_probs(src):
	counts = read_counts(src)
	total = numpy.sum(counts)
	probs = [float(count) / total for count in counts]
	return probs

def compute_gini(src):
	counts = read_counts(src)
	n = 5000
	if n > len(counts):
		n = len(counts)
	counts = random.sample(counts, n)
	gini = 0
	for i in range(len(counts)):
		for j in range(len(counts)):
			if i != j:
				gini += abs(counts[i] - counts[j])
	mean = numpy.mean(counts)
	n2 = len(counts) ** 2
	gini = float(gini) / (2 * n2 * mean)
	return gini

def compute_hhi(src):
	probs = read_counts_as_probs(src)
	hhi = 0
	for p in probs:
		hhi += (p**2)
	return hhi

def compute_entropy(src):
	probs = read_counts_as_probs(src)
	entropy = 0
	for p in probs:
		if p != 0:
			entropy = entropy + (-p*numpy.log2(p))
	return entropy
