import os, numpy, glob, csv

from entropy import compute_entropy, compute_hhi, compute_gini

def compute_entropy_from_over_time_samples(compute_fn=compute_entropy):
	results = {}
	files = glob.glob(os.path.join(os.getenv("TD"), "vm", "level2-domain", "month", "over-time-samples", "categories", "*", "*", "*", "*.txt"))
	for filepath in files:
		remainder, filename = os.path.split(filepath)
		remainder, month = os.path.split(remainder)
		remainder, year = os.path.split(remainder)
		_, cat = os.path.split(remainder)

		if cat not in results:
			results[cat] = []
		results[cat].append(compute_fn(filepath))
	return results

def compute_entropy_from_samples(compute_fn=compute_entropy):
	results = {}
	files = glob.glob(os.path.join(os.getenv("TD"), "vm", "level2-domain", "month", "samples", "*", "*.txt"))
	for filepath in files:
		remainder, _ = os.path.split(filepath)
		_, cat = os.path.split(remainder)
		if cat not in results:
			results[cat] = []
		results[cat].append(compute_fn(filepath))
	return results

if __name__ == "__main__":
	#results = compute_entropy_from_over_time_samples(compute_fn=lambda src: 1.0 / compute_gini(src))
	#results = compute_entropy_from_samples()
	results = compute_entropy_from_over_time_samples(compute_fn=lambda src: 1.0 / compute_hhi(src))
	dest = os.path.join(os.getenv("TR"), "hhi-level2.txt")
	with open(dest, 'w') as destf:
		writer = csv.writer(destf, delimiter="\t")
		cats = sorted(results.keys())
		writer.writerow(['dataset #'] + cats)
		for i in range(len(results[cats[0]])):
			writer.writerow([i] + [results[cat][i] for cat in cats])
