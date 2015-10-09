import sys, os, csv, glob, numpy as np, shutil
from multiprocessing import Pool

sys.path.append(os.getenv("TC"))
from lib import read_aol_data

def compute_user_volume(src, dest):
	print "Reading tweets from", src
	visits = read_aol_data(src)
	
	print "Computing volume."
	volumes = {}
	for visit in visits:
		user = int(visit[0])
		if user not in volumes:
			volumes[user] = 0
		volumes[user] += 1

	with open(dest, 'w') as destf:
		writer = csv.writer(destf, delimiter="\t")
		for user, clicks in volumes.items():
			writer.writerow([user, clicks])

def wrapper(params):
	return compute_user_volume(*params)

def run_in_parallel(files, temp_dir, dest):
	assert not os.path.exists(temp_dir)
	os.makedirs(temp_dir)

	if not os.path.exists(os.path.dirname(dest)):
		os.makedirs(os.path.dirname(dest))

	params = []
	for f in sorted(files):
		temp_dest = os.path.join(temp_dir, os.path.split(f)[1])
		params.append((f, temp_dest))

	print "Computing volumes for each file."
	p = Pool(processes=len(files))
	p.map(wrapper, params)

	print "Combining volume files."
	volumes = {}
	count = 1
	for _, temp_file in params:
		print "Processing", os.path.basename(temp_file), count
		count += 1
		with open(temp_file, 'r') as tempf:
			reader = csv.reader(tempf, delimiter="\t")
			for row in reader:
				user = int(row[0])
				clicks = int(row[1])
				if user not in volumes:
					volumes[user] = 0
				volumes[user] += clicks

	print "Sorting users by volume."
	sorted_volumes = sorted(volumes.items(), key=lambda tupl: tupl[1], reverse=True)

	print "Writing combined volume file."
	with open(dest, "w") as destf:
		writer = csv.writer(destf, delimiter="\t")
		for user, clicks in sorted_volumes:
			writer.writerow([user, clicks])

	print "Deleting temp dir."
	shutil.rmtree(temp_dir)

def main():
	run_in_parallel(
		glob.glob(os.path.join(os.getenv("TD"), "aol", "news-only", "*.txt")),
		os.path.join(os.getenv("TD"), "aol", "temp"),
		os.path.join(os.getenv("TR"), "aol-user-volumes-news-only.txt")
	)
	
if __name__ == "__main__":
	main()
	#debug()
