import sys
import os
import re
import logging
import string
import tarfile
import struct
import csv
from datetime import datetime
from operator import itemgetter
from multiprocessing import Pool

logging.basicConfig(level=logging.DEBUG)

BASE_DATA_DIR = os.path.join('/', 'N', 'dc2', 'projects', 'filter_bubble', 'projects', 'web-traffic', 'data')
AGENTS = [b'B', b'?']
DIRECTIONS = [b'O', b'I']

def hostname(url):
    """
    >>> hostname('http://https://test.com')
    'https'
    >>> hostname('http://test.com?site=https://other.com')
    'test.com'
    >>> hostname('http://test.com/index?test=1')
    'test.com'
    >>> hostname('http://indiana.facebook.com/dir1/page.html')
    'indiana.facebook.com'
    >>> hostname('http://facebook.com')
    'facebook.com'
    >>> hostname('http://www.facebook.com:80/')
    'facebook.com'
    """

    # remove protocol
    idx = url.find(b'://')
    if idx != -1:
        url = url[idx + 3:]

    # remove port
    idx = url.find(b':')
    if idx != -1:
        url = url[:idx]

    # remove trailing / and everything that follows
    idx = url.find(b'/')
    if idx != -1:
        url = url[:idx]

    # remove ? and anything that follows if it wasn't in previous steps
    idx = url.find(b'?')
    if idx != -1:
        url = url[:idx]

    url_is_clean = False
    while not url_is_clean:
        if url.startswith(b'www.'):
            url = url[4:]
        elif url.startswith(b'www2.') or url.startswith(b'www3.'):
            url = url[5:]
        else:
            url_is_clean = True
    return url


def parse_dt_from_filename(filepath):
	m = re.search('(\d{4})\-(\d{2})\-(\d{2})\_(\d{2})\:(\d{2})\:(\d{2})', os.path.basename(filepath))
	assert m is not None and len(m.groups()) == 6
	t = [int(elt) for elt in m.groups()]
	return datetime(*t)


def compute_path_strengths(tar_file):
    paths = {}

    try:
        tar = tarfile.open(tar_file, 'r:gz')
    except Exception as e:
        logging.debug('Could not open {}'.format(tar_file))
        logging.debug('Error: {}'.format(str(e)))
        return paths
    
    for tarinfo in tar:
        logging.debug('Processing {}'.format(tarinfo.name))

        try:
            f = tar.extractfile(tarinfo)
        except Exception as e:
            logging.debug('Could not open {}.'.format(tarinfo.name))
            continue

        # skip header line
        f.readline()
        line_num = 1
        for line in f:
            line_num += 1

            if line.strip() == b'':
                continue

            ad_strings = []
            for a in AGENTS:
                for d in DIRECTIONS:
                    s = a + d
                    if s in line:
                        ad_strings.append(s)

            log_record = False
            
            ad_idx = -1
            if len(ad_strings) == 0:
                logging.debug('Could not find agent/direction: {} (ln: {})'.format(line, line_num))
                continue
            elif len(ad_strings) > 1:
                log_record = True
                string_lengths = []
                logging.debug('Line contains more than one valid agent/direction substring: {} (ln: {})'.format(line, line_num))
                for ads in ad_strings:
                    idx = line.index(ads)
                    string_lengths.append((ads, abs(4-idx), idx))
                sorted_strings = sorted(string_lengths, key=itemgetter(1))
                logging.debug('Choosing {}'.format(sorted_strings[0]))
                agent = chr(sorted_strings[0][0][0])
                direction = chr(sorted_strings[0][0][1])
                ad_idx = sorted_strings[0][2]
            else:
                agent = chr(ad_strings[0][0])
                direction = chr(ad_strings[0][1])
                ad_idx = line.index(ad_strings[0])

            if ad_idx < 0:
                logging.debug('Something is wrong. Could not find agent/direction. File: {}'.format(tarinfo.name))
                logging.debug('Line: {} (ln: {})'.format(line, line_num))
                logging.debug('ad_string: {}, ad_idx: {}'.format(ad_strings, ad_idx))
                exit(1)

            try:
                dt = datetime.fromtimestamp(struct.unpack('<I', line[:ad_idx])[0])
            except:
                #log_record = True
                #logging.debug('Could not parse date. Reverting to filename: {} (ln: {})'.format(line, line_num))
                dt = parse_dt_from_filename(tarinfo.name)

            referrer = line[ad_idx+2:].strip()
            referrer_hostname = hostname(referrer)

            try:
                line_num += 1
                temp_line = f.readline()
                target_hostname = temp_line.strip()
            except Exception as e:
                logging.debug('Could not parse target hostname: {} (ln: {})'.format(temp_line, line_num))
                logging.debug('Error: {}'.format(str(e)))
                exit(1)

            try:
                line_num += 1
                temp_line = f.readline()
                target_path = temp_line.strip()
            except Exception as e:
                logging.debug('Could not parse target path: {} (ln: {})'.format(temp_line, line_num))
                logging.debug('Error: {}'.format(str(e)))
                target_path = ''

            target = target_hostname + target_path

            if referrer_hostname not in paths:
                paths[referrer_hostname] = {}
            if target_hostname not in paths[referrer_hostname]:
                paths[referrer_hostname][target_hostname] = 0
            paths[referrer_hostname][target_hostname] += 1

            if log_record:
                logging.debug('Current record: {line}\n{dt} {agent}{direction}\n{referrer_hostname} {referrer}\n{target_hostname} {target}'.format(
                    line=line,
                    dt=dt,
                    agent=agent,
                    direction=direction,
                    referrer_hostname=referrer_hostname,
                    referrer=referrer,
                    target_hostname=target_hostname,
                    target=target
                ))

    return paths


def save_path_strengths(dest, paths):
    if not os.path.exists(os.path.dirname(dest)):
        os.makedirs(os.path.dirname(dest))

    tuples = []
    
    for referrer in paths:
        for target in paths[referrer]:
            if referrer.strip() != b'' or target.strip() != b'':
                try:
                    tuples.append((referrer, target, paths[referrer][target]))
                except Exception as e:
                    logging.debug('Could not decove referrer or target. Referrer: {}, target: {}'.format(referrer, target))
                    logging.debug('Error: {}'.format(str(e)))

    with open(dest, 'wb') as f:
        f.write(b'referrer\ttarget\tlink weight\n')
        for t in sorted(tuples, key=itemgetter(2), reverse=True):
            try:
                f.write(b'\t'.join([ti if isinstance(ti, bytes) else str(ti).encode() for ti in t]) + b'\n')
            except Exception as e:
                logging.debug('Could not write to output file.')
                logging.debug('Tuple: {}'.format(t))
                logging.debug('Error: {}'.format(str(e)))


def analyze_one():
    filepath = os.path.join(BASE_DATA_DIR, 'sample3', '2008-07-11.click.tar.gz')
    paths = compute_path_strengths(filepath)
    save_path_strengths(os.path.join(BASE_DATA_DIR, 'sample3', 'paths', '2008-07-11.tsv'), paths)


def worker(params):
    paths = compute_path_strengths(params[0])
    save_path_strengths(params[1], paths)


def analyze_all():
    if len(sys.argv) != 3:
        print('You need two arguments. Source dir containing click.tar.gz, and dest dir.')
        exit(1)
        
    params = []
    src_dir = sys.argv[1]
    dest_dir = sys.argv[2]
    for f in os.listdir(src_dir):
        params.append((os.path.join(src_dir, f), os.path.join(dest_dir, '{}.tsv'.format(f.split('.')[0]))))

    p = Pool(processes=16)
    results = p.map(worker, params)


if __name__ == '__main__':
    #analyze_one()
    analyze_all()
