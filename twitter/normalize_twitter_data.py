import sys, os, glob, re
from multiprocessing import Pool

sys.path.append(os.getenv("TC"))
from lib import normalize_url, nth_level_domain

sys.path.append(os.path.join(os.getenv("TC"), "filter"))
from filter_targets import should_skip_host

def read_raw_twitter_data(filepath):
	tweets = []
	with open(filepath, 'r') as f:
		for line in f:
			try:
				parts = line.split("|")

				user = int(parts[0])
				dtstr = parts[1]
				friends = int(parts[-2])
				followers = int(parts[-3])
			
				dest = "".join(parts[2:-3])
				dest = normalize_url(dest)
				tweets.append((user, dest, followers, friends))
			except:
				print "Could not parse line: %s\t%s" % (line, filepath)
	return tweets

def normalize_data_wrapper(param):
	return normalize_data(*param)

def normalize_data(src, dest):
	print "Processing:", src
	visits = read_raw_twitter_data(src)
	if len(visits) > 0:
		with open(dest, "w") as f:
			f.write("user\turl\tfollowers\tfriends\n")
			for visit in visits:
				if not should_skip_host(visit[1]):
					f.write("\t".join(map(str, visit)) + "\n")

def run_in_parallel(files, dest_dir):
	if not os.path.exists(dest_dir):
		os.makedirs(dest_dir)

	params = []
	for f in files:
		_, filename = os.path.split(f)
		m = re.search("address_(\d{4})_(\d{2})_(\d{2}).dat", filename)
		assert m is not None
		new_filename = "%s-%s-%s.dat" % (m.group(1), m.group(2), m.group(3))
		params.append((f, os.path.join(dest_dir, new_filename)))

	p = Pool(processes=16)
	results = p.map(normalize_data_wrapper, params)

def main():
	files = glob.glob(os.path.join(os.getenv("TD"), "tweets", "raw", "*", "*.dat"))
	dest_dir = os.path.join(os.getenv("TD"), "tweets", "normalized")
	run_in_parallel(files, dest_dir)

def debug():
	src = os.path.join(os.getenv("TD"), "tweets", "raw", "2014-04", "address_2014_04_01.dat")
	dest = os.path.join(os.getenv("TD"), "twitter-2014-04-01.dat")
	
	#normalize_data(src, dest)
	run_in_parallel([src], os.path.dirname(dest))

if __name__ == "__main__":
	main()
