import csv, numpy as np, urlparse, fnmatch, string

def binary_search(lst, val):
	"""
	>>> binary_search([], 42)
	>>> binary_search([1, 2, 3, 4], 42)
	>>> binary_search([1, 2, 3, 4, 5], 42)
	>>> binary_search([1, 2, 42, 44, 45], 42)
	2
	>>> binary_search([42, 43, 44, 45, 46], 42)
	0
	>>> binary_search([1, 2, 3, 4, 42], 42)
	4
	>>> binary_search([1, 2, 52, 53, 54], 42)
	>>> binary_search([52, 53, 54, 55, 56], 42)
	>>> binary_search([1, 2, 3, 4, 52], 42)
	"""
	def worker(lst, val, begin, end):
		if len(lst) == 0:
			return None
		elif begin == end and val == lst[begin]:
			return begin
		elif begin == end and val != lst[begin]:
			return None
		else: # begin != end
			offset = (end - begin) / 2
			middle_val = lst[begin + offset]
			if val > middle_val:
				new_begin = begin + offset + 1 if begin + offset + 1 <= end else end
				return worker(lst, val, new_begin, end)
			elif val < middle_val:
				new_end = begin + offset - 1 if begin + offset - 1 >= begin else begin
				return worker(lst, val, begin, new_end)
			else:
				return begin + offset
	return worker(lst, val, 0, len(lst) - 1)
		
def fnmatches_multiple(patterns, s):
	for p in patterns:
		if fnmatch.fnmatch(s, p):
			return True
	return False

def read_volume_data(filepath):
	volumes = []
	with open(filepath, 'r') as f:
		reader = csv.reader(f, delimiter="\t")
		for row in reader:
			volumes.append((int(row[0]), int(row[1])))
	return volumes

def read_twitter_data(filepath):
	visits = []
	with open(filepath, 'r') as f:
		reader = csv.reader(f, delimiter="\t")
		reader.next() # header line
		for row in reader:
			visits.append((int(row[0]), row[1], int(row[2]), int(row[3])))
	return visits

def read_aol_data(filepath):
	visits = []
	with open(filepath, 'r') as f:
		reader = csv.reader(f, delimiter="\t")
		reader.next() # header line
		for row in reader:
			visits.append((int(row[0]), row[1]))
	return visits
	
def read_results_file(filepath):
	labels, data = [], []
	with open(filepath, 'r') as f:
		reader = csv.reader(f, delimiter="\t")
		header = reader.next()
		for row in reader:
			labels.append(row[0])
			data.append([float(r) for r in row[1:]])
		return header, labels, np.array(data).transpose().tolist()

def read_vm_file(filepath):
	data = []
	with open(filepath, 'r') as f:
		prevline = None
		prevprevline = None
		for rawline in f:
			line = rawline.strip()
			if line != '':
				parts = line.split("\t")
				if len(parts) == 3:
					data.append((parts[0], parts[1], int(parts[2])))
				elif len(parts) == 2:
					data.append(('', parts[0], int(parts[1])))
				else:
					print "Invalid line: %s\n%s\n%s\n" % (line, prevline, prevprevline)
			prevprevline = prevline
			prevline = line
	return data

def normalize_url(url):
	"""
	>>> normalize_url('http://https://test.com')
	'https'
	>>> normalize_url('http://test.com?site=https://other.com')
	'test.com'
	>>> normalize_url('http://test.com/index?test=1')
	'test.com'
	>>> normalize_url('http://indiana.facebook.com/dir1/page.html')
	'indiana.facebook.com'
	>>> normalize_url('http://facebook.com')
	'facebook.com'
	>>> normalize_url('http://www.facebook.com:80/')
	'facebook.com'
	"""
	url = filter(lambda char: char in string.printable, url)

	idx = url.find("://")
	if idx != -1:
		url = url[idx + 3:]

	idx = url.find(":")
	if idx != -1:
		url = url[:idx]

	idx = url.find("/")
	if idx != -1:
		url = url[:idx]

	idx = url.find("?")
	if idx != -1:
		url = url[:idx]

	url_is_clean = False
	while not url_is_clean:
		if url.startswith('www.'):
			url = url[4:]
		elif url.startswith('www2.') or url.startswith('www3.'):
			url = url[5:]
		else:
			url_is_clean = True
	
	return url

def is_exception(host):
	"""
	Exceptions are domain names such as google.co.uk or hire.mil.gov, where the top level domain can be thought of co.uk or mil.gov rather than .uk or .gov. These domains need to be processed as a special case when converting the domain level from one level to another, since they are essentially of one level higher than they would ordinarily be thought of. That is, google.co.uk is a 3rd level domain, but for practicel purposes it should be considered a 2nd level domain.

	>>> is_exception('')
	False
	>>> is_exception('google.com')
	False
	>>> is_exception('google.co.uk')
	True
	>>> is_exception('hire.mil.gov')
	True
	>>> is_exception('indiana.edu')
	False
	>>> is_exception('indiana.edu.us')
	True
	>>> is_exception('whitehouse.gov')
	False
	"""
	exceptions = [".com.", ".net.", ".org.", ".edu.", ".mil.", ".gov.", ".co."]
	for e in exceptions:
		if e in host:
			return True
	return False

def is_ip_address(host):
	"""
	>>> is_ip_address('')
	False
	>>> is_ip_address('192.168.2.1')
	True
	>>> is_ip_address('192')
	False
	>>> is_ip_address('192.168')
	False
	>>> is_ip_address('192.168.2')
	False
	>>> is_ip_address('...')
	False
	>>> is_ip_address('asdf.asdf.asdf.asdf')
	False
	>>> is_ip_address('999.0.0.1')
	False
	"""
	parts = host.strip().split('.')
	if len(parts) != 4:
		return False
	else:
		for part in parts:
			if part.isdigit() and int(part) >= 0 and int(part) <= 255:
				pass
			else:
				return False
	return True

def domain_level(host):
	"""
	>>> domain_level('')
	0
	>>> domain_level('    ')
	0
	>>> domain_level('com')
	1
	>>> domain_level('facebook.com')
	2
	>>> domain_level('indiana.facebook.com')
	3
	"""
	if host.strip() == '':
		return 0
	else:
		return len(host.strip().split('.'))

def nth_level_domain(host, n):
	"""
	>>> nth_level_domain('facebook.com', 1)
	'com'
	>>> nth_level_domain('', 2)
	''
	>>> nth_level_domain('facebook.com', 2)
	'facebook.com'
	>>> nth_level_domain('facebook.com', 3)
	'facebook.com'
	>>> nth_level_domain('indiana.facebook.com', 2)
	'facebook.com'
	"""
	parts = host.strip().split('.')
	if len(parts) <= n:
		return host.strip()
	else:
		s = ".".join(n*["%s"])
		new_parts = tuple(parts[-n:])
		return s % new_parts

def change_domain_level(host, domain_level):
	"""
	>>> change_domain_level('192.168.2.1', 2)
	'192.168.2.1'
	>>> change_domain_level('192.168.2', 2)
	'168.2'
	>>> change_domain_level('', 2)
	''
	>>> change_domain_level('indiana.facebook.com', 2)
	'facebook.com'
	>>> change_domain_level('facebook.com', 3)
	'facebook.com'
	>>> change_domain_level('google.co.uk', 2)
	'google.co.uk'
	>>> change_domain_level('google.co.uk', 3)
	'google.co.uk'
	"""
	if is_ip_address(host):
		return host
	elif is_exception(host):
		return nth_level_domain(host, domain_level + 1)
	else:
		return nth_level_domain(host, domain_level)

def parents(url):
	"""
	>>> parents('facebook.com')
	[]
	>>> parents('indiana.facebook.com')
	['facebook.com']
	>>> parents('1.2.3.news.bbc.co.uk')
	['2.3.news.bbc.co.uk', '3.news.bbc.co.uk', 'news.bbc.co.uk', 'bbc.co.uk']
	"""
	parent_urls = []
	dl = domain_level(url)
	if is_exception(url):
		end = 2
	else:
		end = 1
	for parent_dl in range(dl-1, end, -1):
		parent_urls.append(nth_level_domain(url, parent_dl))
	return parent_urls

if __name__ == "__main__":
	print "Testing."
	import doctest
	doctest.testmod()
