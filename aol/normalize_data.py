import sys, os, glob, re, csv
from multiprocessing import Pool

sys.path.append(os.getenv("TC"))
from lib import normalize_url, nth_level_domain

sys.path.append(os.path.join(os.getenv("TC"), "filter"))
from filter_targets import should_skip_host

def read_raw_aol_data(filepath):
	clicks = []
	with open(filepath, 'r') as f:
		reader = csv.reader(f, delimiter="\t")
		reader.next() # skip header
		for row in reader:
			try:
				user = int(row[0])
				dest = normalize_url(row[4])
				clicks.append((user, dest))
			except:
				print "Failed:", row
	return clicks

def normalize_data_wrapper(param):
	return normalize_data(*param)

def normalize_data(src, dest):
	print "Processing:", src
	visits = read_raw_aol_data(src)
	if len(visits) > 0:
		with open(dest, "w") as f:
			writer = csv.writer(f, delimiter="\t")
			writer.writerow(["User", "URL"])
			for visit in visits:
				if not should_skip_host(visit[1]):
					writer.writerow(list(visit))

def main():
	files = glob.glob(os.path.join(os.getenv("TD"), "aol", "raw", "*.txt"))
	dest_dir = os.path.join(os.getenv("TD"), "aol", "normalized")
	if not os.path.exists(dest_dir):
		os.makedirs(dest_dir)

	params = [(f, os.path.join(dest_dir, os.path.split(f)[1])) for f in files]
	p = Pool(processes=16)
	results = p.map(normalize_data_wrapper, params)

if __name__ == "__main__":
	main()
