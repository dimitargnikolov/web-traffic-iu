import os, re, numpy, glob
from multiprocessing import Pool

def compute_entropy_on_json(filepath):
	clicks = []
	with open(filepath, 'r') as f:
		for line in f:
			if "name" in line and "size" in line:
				m = re.search('"size": (\\d+)', line)
				if m is not None:
					count = int(m.group(1))
					clicks.append(count)
	
	total = numpy.sum(clicks)
	probs = [float(count) / total for count in clicks]
	return compute_entropy(probs)

def compute_entropy(probs):
	h = 0
	for p in probs:
		h += -p*numpy.log2(p)
	return h

def choose_representative_jsons():
	files = glob.glob(os.path.join(os.getenv("TR"), "json-week-level2", "*", "*.txt"))
	p = Pool(processes=10)
	entropies = p.map(compute_entropy_on_json, files)
	
	results = {}
	for i in range(len(files)):
		remainder, filename = os.path.split(files[i])
		_, cat = os.path.split(remainder)
		if cat not in results:
			results[cat] = []
		results[cat].append((filename, entropies[i]))

	for cat in results:
		results[cat] = sorted(results[cat], key=lambda tpl: tpl[1])

	start = len(results.values()[0]) / 2 - 2
	end = len(results.values()[0]) / 2 + 2
	#start = 0
	#end = len(results.values()[0])
	for cat in results:
		print cat
		print "\n".join(map(str, results[cat][start:end]))
		print
		print

def test():
	print compute_entropy_on_json(os.path.join(os.getenv("TR"), "json-week-level2", "search", "2007-19.txt"))

if __name__ == "__main__":
	#test()
	choose_representative_jsons()
