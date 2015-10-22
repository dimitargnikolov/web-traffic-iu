import os, numpy, glob, csv

from entropy import compute_entropy

def compute_entropy_from_over_time_samples(files, compute_fn=compute_entropy):
	results = {}
	for filepath in files:
		remainder, filename = os.path.split(filepath)
		remainder, month = os.path.split(remainder)
		remainder, year = os.path.split(remainder)
		_, cat = os.path.split(remainder)

		dt = "%s-%s" % (year, month)
		if dt not in results:
			results[dt] = {}
		if cat not in results[dt]:
			results[dt][cat] = []
		results[dt][cat] = compute_fn(filepath)
	return results

def compute_entropy_from_samples(files, compute_fn=compute_entropy):
	results = {}
	for filepath in files:
		remainder, _ = os.path.split(filepath)
		_, cat = os.path.split(remainder)
		if cat not in results:
			results[cat] = []
		results[cat].append(compute_fn(filepath))
	return results

if __name__ == "__main__":
	files = glob.glob(os.path.join(os.getenv("TD"), "vm", "news", "full-domain", "month", "over-time-samples", "*", "*", "*", "*.txt"))
	results = compute_entropy_from_over_time_samples(files, compute_fn=compute_entropy)
	dates = sorted(results.keys())
	dest = os.path.join(os.getenv("TR"), "news-entropy-full.csv")
	with open(dest, 'w') as destf:
		writer = csv.writer(destf, delimiter="\t")
		cats = sorted(results[dates[0]].keys())
		writer.writerow(['date'] + cats)
		for dt in dates:
			entropies = []
			for cat in cats:
				entropies.append(results[dt][cat])
			writer.writerow([dt] + entropies)
