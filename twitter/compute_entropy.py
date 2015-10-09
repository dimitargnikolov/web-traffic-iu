import sys, os, numpy, csv

sys.path.append(os.getenv("TC"))
from lib import change_domain_level

def entropy(probs):
	entropy = 0
	for p in probs:
		if p != 0:
			entropy = entropy + (-p*numpy.log2(p))
	return entropy
		
def compute_entropy(src, avg_user_dest, collective_dest, url_mod=None):
	print "Reading user clicks for", os.path.basename(src)
	user_clicks = {}
	dests = {}
	with open(src, 'r') as f:
		reader = csv.reader(f, delimiter="\t")
		for row in reader:
			user = int(row[0])
			if url_mod is not None:
				url = url_mod(row[1])
			else:
				url = row[1]
			clicks = int(row[2])
			
			if user not in user_clicks:
				user_clicks[user] = {}
			if url not in user_clicks[user]:
				user_clicks[user][url] = 0
			user_clicks[user][url] += clicks

			if url not in dests:
				dests[url] = 0
			dests[url] += clicks

	print "Computing average user entropy."
	entropies = {}
	for user in user_clicks:
		total = float(numpy.sum(user_clicks[user].values()))
		probs = [count / total for count in user_clicks[user].values()]
		entropies[user] = entropy(probs)

	print "Writing results."
	with open(avg_user_dest, 'w') as destf:
		writer = csv.writer(destf, delimiter="\t")
		for user in entropies:
			writer.writerow([user, entropies[user]])

	print "Computing collective entropy."
	total = float(numpy.sum(dests.values()))
	probs = [count / total for count in dests.values()]
	h = entropy(probs)

	print "Writing result."
	with open(collective_dest, 'w') as destf:
		destf.write(str(h))

if __name__ == "__main__":
	compute_entropy(
		os.path.join(os.getenv("TD"), "tweets", "sample-u150-c100-news-only.txt"), 
		os.path.join(os.getenv("TR"), "twitter", "twitter-avg-user-entropy-u150-c100-news-only.txt"),
		os.path.join(os.getenv("TR"), "twitter", "twitter-collective-entropy-u150-c100-news-only.txt")
	)

	compute_entropy(
		os.path.join(os.getenv("TD"), "aol", "sample-u150-c100-news-only.txt"), 
		os.path.join(os.getenv("TR"), "aol", "aol-avg-user-entropy-u150-c100-news-only.txt"),
		os.path.join(os.getenv("TR"), "aol", "aol-collective-entropy-u150-c100-news-only.txt")
	)

	compute_entropy(
		os.path.join(os.getenv("TD"), "tweets", "sample-u1500-c1000.txt"), 
		os.path.join(os.getenv("TR"), "twitter", "twitter-avg-user-entropy-u1500-c1000.txt"),
		os.path.join(os.getenv("TR"), "twitter", "twitter-collective-entropy-u1500-c1000.txt"),
		url_mod = lambda url: change_domain_level(url, 2)
	)

	compute_entropy(
		os.path.join(os.getenv("TD"), "aol", "sample-u1500-c1000.txt"), 
		os.path.join(os.getenv("TR"), "aol", "aol-avg-user-entropy-u1500-c1000.txt"),
		os.path.join(os.getenv("TR"), "aol", "aol-collective-entropy-u1500-c1000.txt"),
		url_mod = lambda url: change_domain_level(url, 2)
	)
