import sys, os, numpy, csv, glob
from multiprocessing import Pool

sys.path.append(os.getenv("TC"))
from lib import read_vm_file

def merge_vms(files, dest):
	print "Processing %s..." % files[0]
	vms = [read_vm_file(f) for f in files]
	data = {}
	for vm in vms:
		print "Processing next vm."
		for referrer, target, num_clicks in vm:
			if referrer not in data:
				data[referrer] = {}
			if target not in data[referrer]:
				data[referrer][target] = 0
			data[referrer][target] += num_clicks
	
	with open(dest, 'w') as destf:
		writer = csv.writer(destf, delimiter="\t")
		for referrer in data.keys():
			for target in data[referrer].keys():
				writer.writerow([referrer, target, data[referrer][target]])
	
def main():
	files = glob.glob(os.path.join(os.getenv("TD"), "vm", "level2-domain", "month", "unfiltered", "*", "*", "*.txt"))
	dest = os.path.join(os.getenv("TD"), "vm", "level2-unfiltered.txt")
	merge_vms(files, dest)

if __name__ == "__main__":
	main()
