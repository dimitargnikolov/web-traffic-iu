import os, numpy, csv, glob, re
from multiprocessing import Pool

from entropy import compute_entropy 

def run_in_parallel(src_files, dest):
	p = Pool(processes=10)
	results = p.map(compute_entropy, src_files)

	print "Reducing results."
	cats = set()
	entropies = {}
	for i in range(len(src_files)):
		remainder, filename = os.path.split(src_files[i])
		m = re.search(r'\D+\-(\d+)\-\D+\.txt', filename)
		if m is None:
			print "Couldn't process", src_files[i]
			continue

		num_clicks = int(m.group(1))

		_, category = os.path.split(remainder)

		if num_clicks not in entropies:
			entropies[num_clicks] = {}
		assert category not in entropies[num_clicks]
		entropies[num_clicks][category] = results[i]

		if category not in cats:
			cats.add(category)

	if not os.path.exists(os.path.dirname(dest)):
		os.makedirs(os.path.dirname(dest))
	cats = sorted(list(cats))
	with open(dest, 'w') as destf:
		writer = csv.writer(destf, delimiter="\t")
		writer.writerow(['num clicks'] + cats)
		for num_clicks in sorted(entropies.keys()):
			currrow = [num_clicks]
			for cat in cats:
				currrow.append(entropies[num_clicks][cat])
			writer.writerow(currrow)

def main():
	files = glob.glob(os.path.join(os.getenv("TD"), "vm", "level2-domain", "month", "entropy-vs-volume-new", "categories", "*", "*.txt"))
	dest = os.path.join(os.getenv("TR"), "evstv-level2-month-new.txt")
	if not os.path.exists(os.path.dirname(dest)):
		os.makedirs(os.path.dirname(dest))
	run_in_parallel(files, dest)

def test():
	files = glob.glob(os.path.join(os.getenv("TD"), "vm", "test", "output", "multiple-samples", "*", "*.txt"))
	dest = os.path.join(os.getenv("TR"), "test", "entropy-vs-traffic-volume.txt")
	if not os.path.exists(os.path.dirname(dest)):
		os.makedirs(os.path.dirname(dest))
	run_in_parallel(files, dest)

if __name__ == "__main__":
	#test()
	main()
