import sys, os, struct, re, datetime, time, glob, csv
from multiprocessing import Pool

sys.path.append(os.path.join(os.path.dirname(os.path.join(os.getcwd(), __file__)), '..'))
from lib import normalize_url

# reading a request is accomplished in three stages:
# 1. in the first, the timestamp, direction, agent and referrer are read
# 2. in the second, the host is read
# 3. in the third, the requested file path is read
READING_REFERRER = 0
READING_TARGET = 1
READING_FILEPATH = 2
			
# indices of the appropriate fields in the request tuple	
TIMESTAMP = 0
REFERRER = 1
TARGET = 2
DIRECTION = 3
AGENT = 4

SKIP_LINES = ['?'*n for n in range(1, 6)]

def parse_dt_from_filename(filepath):
	m = re.search("(\d{4})\-(\d{2})\-(\d{2})\_(\d{2})\:(\d{2})\:(\d{2})", os.path.basename(filepath))
	assert m is not None and len(m.groups()) == 6
	t = [int(elt) for elt in m.groups()]
	return datetime.datetime(*t)

def validate_requests(src, valid_dest, invalid_dest):
	print "Processing", src

	# extract a timestamp from from the filename to validate against
	file_dt = parse_dt_from_filename(src)
	
	with open(valid_dest, 'w') as valid_destf:
		valid_writer = csv.writer(valid_destf, delimiter="\t")

		with open(invalid_dest, 'w') as invalid_destf:

			with open(src, 'r') as f:
				stage = READING_REFERRER

				# a request is [timestamp, referrer, target, direction, agent]
				request = [None, None, None, None, None]
		
				# discard the header line
				f.readline()

				out_of_sync = False

				prev = None
				prevprev = None
				for rawline in f:
					line = rawline.strip()
					if line in SKIP_LINES:
						continue

					# if somehow we got out of sync reading the file
					# try to find a referrer line
					if out_of_sync:
						stage = READING_REFERRER

					# reading the first line of a request consisting of: XXXXAD[R] where
					# XXXX is the timestamp in little endian order
					# A is the agent and can be either 'B' for browser or '?' for unknown
					# D is the direction, 'I' for traffic going into IU, 'O' for traffic going outside IU
					# R is the referrer
					if stage == READING_REFERRER:
						# it's possible to have new lines between records
						if line == '':
							continue

						for s in ['BI', 'BO', '?I', '?O']:
							idx = line.find(s)
							if idx != -1:
								out_of_sync = False
								try: 
									request[TIMESTAMP] = struct.unpack('<I', line[:idx])[0]
								except:
									request[TIMESTAMP] = time.mktime(file_dt.timetuple())
								request[AGENT] = line[idx]
								request[DIRECTION] = line[idx+1]
								request[REFERRER] = normalize_url(line[idx+2:]).split('/')[0]
								break
							else:
								out_of_sync = True
						stage = READING_TARGET
				
					# reading the requested host
					elif stage == READING_TARGET:
						request[TARGET] = normalize_url(line.split('/')[0])
						stage = READING_FILEPATH
			
					# reading the requested file
					# after this step, the reading of the request is done and it
					# can be matched to any supplied criteria
					elif stage == READING_FILEPATH:
						is_valid = True
						for val in request:
							if val is None:
								is_valid = False
								break

						if is_valid:
							dt = datetime.datetime.fromtimestamp(request[TIMESTAMP])

							# if the difference between the file's and the record's timestamps
							# is more than one day, use the file time stamp
							tdelta = dt - file_dt
							if tdelta.days < 0:
								tdelta = -tdelta
							if tdelta.seconds / 60.0 / 60.0 > 1:
								request[TIMESTAMP] = time.mktime(file_dt.timetuple())

							is_valid = (request[AGENT] == 'B' or request[AGENT] == '?') and \
								(request[DIRECTION] == 'I' or request[DIRECTION] == 'O')

						if is_valid:
							if request[AGENT] == 'B' and request[DIRECTION] == 'O':
								valid_writer.writerow([
									request[TIMESTAMP], 
									request[REFERRER], 
									request[TARGET]
								])
						else:
							invalid_destf.write("%s\n" % prevprev)
							invalid_destf.write("%s\n" % prev)
							invalid_destf.write("%s\n" % line)

						# reset the variables describing the request
						request = [None, None, None, None, None]
						stage = READING_REFERRER
				
					# we should never get here
					else:
						raise ValueError("Invalid stage: %d" % (stage))

					prevprev = prev
					prev = line

def worker(params):
	return validate_requests(*params)

def run_in_parallel(files, valid_dir, invalid_dir, num_processes):
	params = []
	for f in files:
		remainder, filename = os.path.split(f)
		remainder, month = os.path.split(remainder)
		_, year = os.path.split(remainder)
		
		valid_f = os.path.join(valid_dir, year, month, filename)
		if not os.path.exists(os.path.dirname(valid_f)):
			os.makedirs(os.path.dirname(valid_f))

		invalid_f = os.path.join(invalid_dir, year, month, filename)
		if not os.path.exists(os.path.dirname(invalid_f)):
			os.makedirs(os.path.dirname(invalid_f))
		
		params.append((f, valid_f, invalid_f))
	
	p = Pool(processes=num_processes)
	results = p.map(worker, params)

def test1():
	validate_requests(
		os.path.join(os.getenv("TD"), "sample", "original-requests", "2007", "05", "2007-05-19_00:00:00_+3600.click"), 
		os.path.join(os.getenv("TD"), "valid.txt"), 
		os.path.join(os.getenv("TD"), "invalid.txt")
	)

def test2():
	files = glob.glob(os.path.join(os.getenv("TD"), "sample", "original-requests", "*", "*", "*"))
	valid_dir = os.path.join(os.getenv("TD"), "sample", "valid-requests")
	invalid_dir = os.path.join(os.getenv("TD"), "sample", "invalid-requests")
	run_in_parallel(files, valid_dir, invalid_dir, 2)

def main():
	files = glob.glob(os.path.join(os.getenv("TD"), "original-requests", "*", "*", "*"))
	valid_dir = os.path.join(os.getenv("TD"), "valid-requests")
	invalid_dir = os.path.join(os.getenv("TD"), "invalid-requests")
	run_in_parallel(files, valid_dir, invalid_dir, 16)

if __name__ == "__main__":
	main()
