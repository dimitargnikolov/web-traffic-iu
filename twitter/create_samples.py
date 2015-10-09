import sys, os, random, glob, csv, numpy as np
from multiprocessing import Pool

sys.path.append(os.getenv("TC"))
from lib import read_twitter_data, read_volume_data

def select_users(volume_filepath, num_users, min_num_clicks):
	volumes = read_volume_data(volume_filepath)
	users = []
	for user, num_clicks in volumes:
		if num_clicks >= min_num_clicks:
			users.append(user)
	return set(random.sample(users, num_users))

def select_clicks(user_clicks, num_clicks):
	sample = {}
	for user, dests in user_clicks.items():
		sample[user] = {}
		sampled_dests = random.sample(dests, num_clicks)
		for dest in sampled_dests:
			if dest not in sample[user]:
				sample[user][dest] = 0
			sample[user][dest] += 1
	return sample

def read_users_data(files, users_to_sample):
	data = {}
	for filepath in files:
		with open(filepath, 'r') as f:
			reader = csv.reader(f, delimiter="\t")
			reader.next()
			for row in reader:
				user = int(row[0])
				dest = row[1]
				if user in users_to_sample:
					if user not in data:
						data[user] = []
					data[user].append(dest)
	return data

def sample(tweet_files, volume_file, target_file, num_users, num_clicks):
	print "Sampling users."
	users = select_users(volume_file, num_users, num_clicks)
	
	print "Reading user clicks."
	clicks = read_users_data(tweet_files, users)

	print "Sampling clicks."
	user_sample = select_clicks(clicks, num_clicks)
		
	print "Writing the user sample to disk."
	with open(target_file, 'w') as f:
		writer = csv.writer(f, delimiter="\t")
		for user, dests in user_sample.items():
			for dest, num_clicks in dests.items():
				writer.writerow([user, dest, num_clicks])

def main():
	tweet_files = glob.glob(os.path.join(os.getenv("TD"), "tweets", "news-only", "*"))
	users_volumes_file = os.path.join(os.getenv("TR"), "twitter", "twitter-user-volumes-news-only.txt")

	# (num users, num clicks per user)
	sample_params = [(10000, 10)]
	for num_users, num_clicks in sample_params:
		print "Generating sample with %d users and %d clicks" % (num_users, num_clicks)
		target_file = os.path.join(os.getenv("TD"), "tweets", "sample-u%d-c%d-news-only.txt" % (num_users, num_clicks))
		sample(tweet_files, users_volumes_file, target_file, num_users, num_clicks)

def debug():
	pass

if __name__ == "__main__":
	main()
	#debug()
