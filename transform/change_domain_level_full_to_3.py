import os, glob
from multiprocessing import Pool
from change_domain_level import worker
	
if __name__ == "__main__":
	params = []
	files = glob.glob(os.path.join(os.getenv("TD"), "vm", "full-domain", "hour", "*", "*", "*.txt"))
	dest_dir = os.path.join(os.getenv("TD"), "vm", "level3-domain", "hour")
	for f in files:
		remainder, filename = os.path.split(f)
		remainder, month = os.path.split(remainder)
		remainder, year = os.path.split(remainder)
		destf = os.path.join(dest_dir, year, month, filename)
		if not os.path.exists(os.path.dirname(destf)):
			os.makedirs(os.path.dirname(destf))
		params.append((f, destf, 3))

	p = Pool(processes=16)
	results = p.map(worker, params)
