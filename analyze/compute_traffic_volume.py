import sys, os, numpy, csv, glob
from multiprocessing import Pool

sys.path.append(os.path.join(os.path.dirname(os.path.join(os.getcwd(), __file__)), '..'))
from lib import read_vm_file

def compute_traffic_volume(src):
	print "Processing %s" % src
	total = 0
	data = read_vm_file(src)
	for referrer, target, num_clicks in data:
		total += num_clicks
	return total

def run_in_parallel(src_files, dest, num_processes):
	p = Pool(processes=num_processes)
	results = p.map(compute_traffic_volume, src_files)

	print "Reducing results."
	cats = set()
	volumes = {}
	for i in range(len(src_files)):
		remainder, filename = os.path.split(src_files[i])
		remainder, month = os.path.split(remainder)
		remainder, year = os.path.split(remainder)
		_, category = os.path.split(remainder)

		datestr = os.path.splitext(filename)[0]
		if datestr not in volumes:
			volumes[datestr] = {}
		assert category not in volumes[datestr]
		volumes[datestr][category] = results[i]

		if category not in cats:
			cats.add(category)

	if not os.path.exists(os.path.dirname(dest)):
		os.makedirs(os.path.dirname(dest))
	cats = sorted(list(cats))
	with open(dest, 'w') as destf:
		writer = csv.writer(destf, delimiter="\t")
		writer.writerow(['date'] + cats)
		for datestr in sorted(volumes.keys()):
			currrow = [datestr]
			for cat in cats:
				currrow.append(volumes[datestr][cat])
			writer.writerow(currrow)
	
def main():
	files = glob.glob(os.path.join(os.getenv("TD"), "vm", "level2-domain", "month", "categories", "*", "*", "*", "*.txt"))
	dest = os.path.join(os.getenv("TR"), "volume-level2-month.csv")
	run_in_parallel(files, dest, 16)

def test():
	files = glob.glob(os.path.join(os.getenv("TD"), "vm", "test", "level2-domain", "month", "categories", "*", "*", "*", "*.txt"))
	dest = os.path.join(os.getenv("TR"), "test", "output", "traffic-volume-alt.txt")
	if not os.path.exists(os.path.dirname(dest)):
		os.makedirs(os.path.dirname(dest))
	run_in_parallel(files, dest)

if __name__ == "__main__":
	main()
