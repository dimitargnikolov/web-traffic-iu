import sys, os, glob, numpy
from multiprocessing import Pool

sys.path.append(os.path.join(os.path.dirname(os.path.join(os.getcwd(), __file__)), '..'))
from lib import read_vm_file

def convert_to_json(src, dest):
	print "Processing %s" % src
	data = read_vm_file(src)
	click_counts = {}
	for referrer, target, num_clicks in data:
		if target not in click_counts:
			click_counts[target] = 0
		click_counts[target] += num_clicks

	total_count = float(numpy.sum(click_counts.values()))
	sorted_counts = sorted(click_counts.items(), key=lambda tupl: tupl[1], reverse=True)
	
	cum_count = 0
	json = ""
	for target, count in sorted_counts:
		cum_count += count
		json += '        {"name": "%s", "size": %d}' % (target, count)
		if cum_count / total_count > .3:
			json += "\n"
			break
		else:
			json += ",\n"
		
	with open(dest, 'w') as destf:
		destf.write("""{
    "name": "entropy",
    "children": [
%s
    ]
}
""" % json)

def worker(params):
	return convert_to_json(*params)

def run_in_parallel(files, dest_dir, num_processes):
	params = []
	for f in files:
		remainder, filename = os.path.split(f)
		remainder, month = os.path.split(remainder)
		remainder, year = os.path.split(remainder)
		_, category = os.path.split(remainder)
		destf = os.path.join(dest_dir, category, filename)
		if not os.path.exists(os.path.dirname(destf)):
			os.makedirs(os.path.dirname(destf))
		params.append((f, destf))

	p = Pool(processes=num_processes)
	results = p.map(worker, params)

def test():
	destf = os.path.join(os.getenv("TR"), "test", "2007-19.json")
	if not os.path.exists(os.path.dirname(destf)):
		os.makedirs(os.path.dirname(destf))
	convert_to_json(
		os.path.join(os.getenv("TD"), "vm", "level2-domain", "week", "categories", "search", "2007", "05", "2007-19.txt"),
		destf
	)

def main():
	run_in_parallel(
		glob.glob(os.path.join(os.getenv("TD"), "vm", "level2-domain", "week", "categories", "*", "*", "*", "*.txt")),
		os.path.join(os.getenv("TR"), "json-week-level2"),
		16
	)

if __name__ == "__main__":
	main()
