import sys, os, matplotlib, argparse, csv, math, numpy, re

matplotlib.use('Agg')

from pylab import rand, ones, concatenate
from scipy.stats import scoreatpercentile
from matplotlib.backends.backend_pdf import PdfPages
import matplotlib.pyplot as plt
from matplotlib.ticker import MaxNLocator

from matplotlib import rcParams
rcParams.update({'figure.autolayout': True})

BOX_TOP_PCTL = 75
BOX_BOTTOM_PCTL = 25
WHISKER_TOP_PCTL = 99
WHISKER_BOTTOM_PCTL = 1

COLLECTIVEH_COLORS = {'junk2': '#fdb863', 'social': '#e66101', 'search': '#b2abd2', 'junk1': '#5e3c99'}
USERH_COLORS = {'twitter': '#58aad9', 'aol': '#ffc945'}

def draw_box(ax, index, median, mean, top, bottom, whisker_top, whisker_bottom, outliers=None, 
			 box_color="black", median_color="black", mean_color="black", whisker_color="black", fill_color="white", outlier_color="black"):
	box_width = 0.005
	whisker_width = .3

	# draw the box
	ax.broken_barh([(index - (box_width / 2), box_width)], (bottom, top - bottom), facecolor=fill_color, edgecolor=box_color)

	# draw the median line
	if median is not None:
		ax.broken_barh([(index - (box_width / 2), box_width)], (median, 0), facecolor=fill_color, edgecolor=median_color)
	
	# draw the mean
	if mean is not None:
		ax.plot(index, mean, marker='o', markeredgecolor=mean_color, markerfacecolor='none')
        
	# draw the top whisker
	if whisker_top is not None:
		ax.broken_barh([(index - (whisker_width / 2), whisker_width)], (whisker_top, 0), facecolor=fill_color, edgecolor=whisker_color, linewidth=2)

	# draw the bottom whisker
	if whisker_bottom is not None:
		ax.broken_barh([(index - (whisker_width / 2), whisker_width)], (whisker_bottom, 0), facecolor=fill_color, edgecolor=whisker_color, linewidth=2)

	# draw the line joining the top whisker and the box
	if whisker_top is not None:
		ax.broken_barh([(index, 0)], (whisker_top, top - whisker_top), edgecolor=box_color, linestyle="-")

	# draw the line joining the bottom whisker and the box
	if whisker_bottom is not None:
		ax.broken_barh([(index, 0)], (whisker_bottom, bottom - whisker_bottom), edgecolor=box_color, linestyle="-")
	
	# draw the outliers
	if outliers is not None:
		for p in outliers:
			ax.plot(index, p, color=outlier_color, marker='.')

	# display a subfigure id if this is going to be part of a subfigure
	#plt.figtext(0.025, 0.025, "a", fontsize=30, weight='bold')

def draw_small_boxes(ys, ax):
	count = 0
	for i in ['twitter', 'aol']:
		mean = numpy.mean(ys[i])
		median = None
		
		ste = numpy.std(ys[i]) / numpy.sqrt(len(ys[i]))
		top = mean + 2*ste
		bottom = mean - 2*ste
		
		whisker_top = None
		whisker_bottom = None

		outliers = None

		draw_box(ax, count + 0.01, median, mean, top, bottom, whisker_top, whisker_bottom, fill_color=USERH_COLORS[i])
		count += 0.01

def draw_boxes(ys, ax):
	count = 0
	for i in ['twitter', 'aol']:
		mean = numpy.mean(ys[i])
		median = scoreatpercentile(ys[i], 50)
		
		top = scoreatpercentile(ys[i], BOX_TOP_PCTL)
		bottom = scoreatpercentile(ys[i], BOX_BOTTOM_PCTL)
		
		whisker_top = scoreatpercentile(ys[i], WHISKER_TOP_PCTL)
		whisker_bottom = scoreatpercentile(ys[i], WHISKER_BOTTOM_PCTL)

		outliers = [p for p in ys[i] if p > whisker_top or p < whisker_bottom]

		#draw_box(ax, count + 1, median, mean, top, bottom, whisker_top, whisker_bottom, outliers, fill_color=USERH_COLORS[i])
		draw_box(ax, count + 0.01, median, mean, top, bottom, whisker_top, whisker_bottom, fill_color=USERH_COLORS[i])
		count += 0.01

def read_results(filepath):
	data = []
	with open(filepath, 'r') as f:
		reader = csv.reader(f, delimiter="\t")
		for row in reader:
			data.append(float(row[1]))
	return data

def pbox_plot(twitter_avg_user_h_file, twitter_collective_h_file, 
			  aol_avg_user_h_file, aol_collective_h_file,
			  dest, 
			  twitter_news_avg_user_h_file=None, twitter_news_collective_h_file=None,
			  aol_news_avg_user_h_file=None, aol_news_collective_h_file=None,
			  order=None, title=None, xlabel=None, ylabel=None,
			  inset_results_file=None, inset_title=None):
	print "starting"
	avg_user_hs = {
		'twitter': read_results(twitter_avg_user_h_file),
		'aol': read_results(aol_avg_user_h_file)
	}
	collective_hs = {
		'twitter': float(open(twitter_collective_h_file, 'r').read().strip()),
		'aol': float(open(aol_collective_h_file, 'r').read().strip())
	}
	xlabels = ['Twitter', 'AOL']

	print "data read"

	fig = plt.figure()
	ax = fig.add_subplot(1,1,1)
	ax.scatter(x, y)

	draw_small_boxes(avg_user_hs, ax)
	
	# plot the collective entropies
	ax.plot(0.01, collective_hs['twitter'], marker='^', color="#000000", markerfacecolor=USERH_COLORS['twitter'], markersize=22)
	ax.plot(0.02, collective_hs['aol'], marker='^', color="#000000", markerfacecolor=USERH_COLORS['aol'], markersize=22)

	print "boxes drawn"

	plt.xticks(numpy.arange(0.01, len(xlabels) * 0.01 + 0.01, 0.01), xlabels)
	ax.set_ylim([1, 11])
	ax.yaxis.set_major_locator(MaxNLocator(nbins=7))
	
	for tick in ax.xaxis.get_major_ticks():
		tick.label.set_fontsize(22)
		
	for tick in ax.yaxis.get_major_ticks():
		tick.label.set_fontsize(22)
	
	if xlabel is not None:
		ax.set_xlabel(xlabel, fontsize=22)
		
	if ylabel is not None:
		ax.set_ylabel(ylabel, fontsize=22)

	if title is not None: 
		plt.title(title, fontsize=22)

	# plot the inset
	if twitter_news_avg_user_h_file is not None:
		inset_ax = plt.axes([.61, .10, .35, .34])

		avg_user_hs_news = {
			'twitter': read_results(twitter_news_avg_user_h_file),
			'aol': read_results(aol_news_avg_user_h_file)
			}
		collective_hs_news = {
			'twitter': float(open(twitter_news_collective_h_file, 'r').read().strip()),
			'aol': float(open(aol_news_collective_h_file, 'r').read().strip())
			}
		xlabels = ['Twitter', 'AOL']

		draw_small_boxes(avg_user_hs_news, inset_ax)
	
		# plot the collective entropies
		inset_ax.plot(0.01, collective_hs_news['twitter'], marker='^', color="#000000", markerfacecolor=USERH_COLORS['twitter'], markersize=10)
		inset_ax.plot(0.02, collective_hs_news['aol'], marker='^', color="#000000", markerfacecolor=USERH_COLORS['aol'], markersize=10)

		inset_ax.axes.get_xaxis().set_visible(False)

		if inset_title is not None:
			inset_ax.set_title(inset_title)

	print "saving"
	pp = PdfPages(dest)	
	fig.savefig(pp, format='pdf')
	pp.close()
	plt.close()

if __name__ == "__main__":
	pbox_plot(
		os.path.join(os.getenv("TR"), "twitter", "twitter-avg-user-entropy-u1500-c1000.txt"),
		os.path.join(os.getenv("TR"), "twitter", "twitter-collective-entropy-u1500-c1000.txt"),
		os.path.join(os.getenv("TR"), "aol", "aol-avg-user-entropy-u1500-c1000.txt"),
		os.path.join(os.getenv("TR"), "aol", "aol-collective-entropy-u1500-c1000.txt"),
		os.path.join(os.getenv("TP"), "twitter-aol-entropy.pdf"),
		ylabel="Entropy"#,
		#twitter_news_avg_user_h_file=os.path.join(os.getenv("TR"), "twitter", "twitter-avg-user-entropy-u150-c100-news-only.txt"),
		#twitter_news_collective_h_file=os.path.join(os.getenv("TR"), "twitter", "twitter-collective-entropy-u150-c100-news-only.txt"),
		#aol_news_avg_user_h_file=os.path.join(os.getenv("TR"), "aol", "aol-avg-user-entropy-u150-c100-news-only.txt"),
		#aol_news_collective_h_file=os.path.join(os.getenv("TR"), "aol", "aol-collective-entropy-u150-c100-news-only.txt")
    )
