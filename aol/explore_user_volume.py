import os, csv

if __name__ == "__main__":
	num_clicks = 150
	with open(os.path.join(os.getenv('TR'), 'aol', 'aol-user-volumes-news-only.txt'), 'r') as volumef:
		reader = csv.reader(volumef, delimiter="\t")
		count = 0
		for row in reader:
			curr_clicks = int(row[1])
			if curr_clicks >= num_clicks:
				count += 1
			else:
				break
		print count
