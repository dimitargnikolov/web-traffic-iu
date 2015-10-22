import sys, os, numpy, csv, glob
from multiprocessing import Pool

from entropy import compute_entropy

sys.path.append(os.getenv("TC"))
from lib import read_vm_file

def run_in_parallel(src_files, dest):
	p = Pool(processes=10)
	results = p.map(compute_entropy, src_files)

	print "Reducing results."
	cats = set()
	entropies = {}
	for i in range(len(src_files)):
		remainder, filename = os.path.split(src_files[i])
		remainder, month = os.path.split(remainder)
		remainder, year = os.path.split(remainder)
		_, category = os.path.split(remainder)

		datestr = "%s-%s" % (year, month)
		
		if datestr not in entropies:
			entropies[datestr] = {}
		assert category not in entropies[datestr]
		entropies[datestr][category] = results[i]

		if category not in cats:
			cats.add(category)

	if not os.path.exists(os.path.dirname(dest)):
		os.makedirs(os.path.dirname(dest))
	cats = sorted(list(cats))
	with open(dest, 'w') as destf:
		writer = csv.writer(destf, delimiter="\t")
		writer.writerow(['date'] + cats)
		for datestr in sorted(entropies.keys()):
			currrow = [datestr]
			for cat in cats:
				currrow.append(entropies[datestr][cat])
			writer.writerow(currrow)

def test():
	files = glob.glob(os.path.join(os.getenv("TD"), "vm", "test", "level3-domain", "month", "clean-categories", "*", "*", "*", "*"))
	dest = os.path.join(os.getenv("TR"), "test", "entropy.txt")
	if not os.path.exists(os.path.dirname(dest)):
		os.makedirs(os.path.dirname(dest))
	run_in_parallel(files, dest)

def main():
	files = glob.glob(os.path.join(os.getenv("TD"), "vm", "level2-domain", "month", "smooth", "categories", "*", "*", "*", "*.txt"))
	dest = os.path.join(os.getenv("TR"), "smooth-news-full-month-entropy.txt")
	run_in_parallel(files, dest)

if __name__ == "__main__":
	main()
