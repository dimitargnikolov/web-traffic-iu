import sys, os, matplotlib, argparse, csv, math, numpy, re

matplotlib.use('Agg')

from pylab import rand, ones, concatenate
from scipy.stats import scoreatpercentile
from matplotlib.backends.backend_pdf import PdfPages
import matplotlib.pyplot as plt
from matplotlib.ticker import MaxNLocator

from matplotlib import rcParams
rcParams.update({'figure.autolayout': True})

sys.path.append(os.path.join(os.path.dirname(os.path.join(os.getcwd(), __file__)), '..'))
from lib import read_results_file

BOX_TOP_PCTL = 75
BOX_BOTTOM_PCTL = 25
WHISKER_TOP_PCTL = 99
WHISKER_BOTTOM_PCTL = 1

BOX_COLORS = ['#fdb863', '#e66101', '#b2abd2', '#5e3c99']

def draw_box(ax, index, median, mean, top, bottom, whisker_top, whisker_bottom, outliers, 
			 box_color="black", median_color="black", mean_color="black", whisker_color="black", fill_color="white", outlier_color="black"):
	box_width = .6
	whisker_width = .3

	# draw the box
	ax.broken_barh([(index - (box_width / 2), box_width)], (bottom, top - bottom), facecolor=fill_color, edgecolor=box_color)

	# draw the median line
	ax.broken_barh([(index - (box_width / 2), box_width)], (median, 0), facecolor=fill_color, edgecolor=median_color)
	
	# draw the mean
	ax.plot(index, mean, marker='o', markeredgecolor=mean_color, markerfacecolor='none')
        
	# draw the top whisker
	ax.broken_barh([(index - (whisker_width / 2), whisker_width)], (whisker_top, 0), facecolor=fill_color, edgecolor=whisker_color, linewidth=2)

	# draw the bottom whisker
	ax.broken_barh([(index - (whisker_width / 2), whisker_width)], (whisker_bottom, 0), facecolor=fill_color, edgecolor=whisker_color, linewidth=2)

	# draw the line joining the top whisker and the box
	ax.broken_barh([(index, 0)], (whisker_top, top - whisker_top), edgecolor=box_color, linestyle="-")

	# draw the line joining the bottom whisker and the box
	ax.broken_barh([(index, 0)], (whisker_bottom, bottom - whisker_bottom), edgecolor=box_color, linestyle="-")
	
	# draw the outliers
	for p in outliers:
		ax.plot(index, p, color=outlier_color, marker='.')

	# display a subfigure id if this is going to be part of a subfigure
	plt.figtext(0.025, 0.025, "b", fontsize=30, weight='bold')

def draw_boxes(ys, ax):
	for i in range(len(ys)):
		mean = numpy.mean(ys[i])
		median = scoreatpercentile(ys[i], 50)
		
		top = scoreatpercentile(ys[i], BOX_TOP_PCTL)
		bottom = scoreatpercentile(ys[i], BOX_BOTTOM_PCTL)
		
		whisker_top = scoreatpercentile(ys[i], WHISKER_TOP_PCTL)
		whisker_bottom = scoreatpercentile(ys[i], WHISKER_BOTTOM_PCTL)

		outliers = [p for p in ys[i] if p > whisker_top or p < whisker_bottom]

		draw_box(ax, i + 1, median, mean, top, bottom, whisker_top, whisker_bottom, outliers, fill_color=BOX_COLORS[i])

def pbox_plot(results_file, dest, order=None, inset_results_file=None, inset_title=None, title=None, xlabel=None, ylabel=None):
	headers, labels, ys = read_results_file(results_file)
	cats = headers[1:]
	x = range(len(labels))

	# reorder the data that will be plotted and set legend labels
	xlabels = []
	if order is not None:
		new_ys = []
		for catid, catname in order:
			new_ys.append(ys[cats.index(catid)])
			xlabels.append(catname)
		ys = new_ys
	else:
		xlabels = cats[:]

	fig = plt.figure()
	ax = fig.add_subplot(1,1,1)
	ax.yaxis.grid(color="#bbbbbb")

	draw_boxes(ys, ax)

	plt.xticks(range(1, len(xlabels) + 1), xlabels)
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
	if inset_results_file is not None:
		inset_ax = plt.axes([.61, .10, .35, .34])

		inset_headers, inset_labels, inset_ys = read_results_file(inset_results_file)

		if order is not None:
			new_inset_ys = []
			for catid, catname in order:
				new_inset_ys.append(inset_ys[cats.index(catid)])
			inset_ys = new_inset_ys

		draw_boxes(inset_ys, inset_ax)
		inset_ax.axes.get_xaxis().set_visible(False)

		if inset_title is not None:
			inset_ax.set_title(inset_title)

	pp = PdfPages(dest)	
	fig.savefig(pp, format='pdf')
	pp.close()
	plt.close()

if __name__ == "__main__":
	pbox_plot(
		os.path.join(os.getenv("TR"), "hhi-level2.txt"),
		os.path.join(os.getenv("TP"), "hhi-level2.pdf"), 
		ylabel="Inverse HHI", order=[('email', 'Mail'), ('social', 'Social Media'), ('search', 'Search')]
#		inset_results_file=os.path.join(os.getenv("TR"), "news-full-month-entropy.txt"), 
#		inset_title="News"
    )
