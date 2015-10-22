import sys, os, matplotlib, numpy as np, re, math, csv, calendar

matplotlib.use('Agg')

import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib.font_manager import FontProperties
from matplotlib.ticker import MaxNLocator, LinearLocator

from matplotlib import rcParams
rcParams.update({'figure.autolayout': True})

sys.path.append(os.getenv("TC"))
from lib import read_results_file

COLORS = ['#fdb863', '#e66101', '#b2abd2', '#5e3c99']
MARKERS = ['s', 'o', '^', 'x']

def create_nice_labels(labels):
	nice_labels = []
	for label in labels:
		year, month = map(int, re.match("(\d+)\-(\d+)", label).groups())
		nice_labels.append(calendar.month_abbr[month] + " " + str(year)[2:])
	return nice_labels

def plot_areas(x, ys, ax):
	# this draws the regions and fills them with the appropriate color
	# since matplotlib's 
	yprev = None
	curves = []
	for i in range(len(ys)):
		if yprev is not None:
			for j in range(len(ys[i])):
				ys[i][j] += yprev[j]

		if yprev is not None:
			ax.fill_between(x, ys[i], yprev, color=COLORS[i])
		else:
			ax.fill_between(x, ys[i], 0, color=COLORS[i])
		
		curve = ax.plot(x, ys[i], linestyle="-", color=COLORS[i])[0]
		curves.append(curve)
		ax.plot(x, ys[i], linestyle="-", color="black")

		yprev = ys[i]
	return curves

def line_plot(result_file, dest, order=None, inset_result_file=None, inset_title=None, title=None, legend_pos=None, legend_cols=None, xlabel=None, ylabel=None):
	headers, labels, ys = read_results_file(result_file)
	cats = headers[1:]
	x = range(len(labels))

	# reorder the data that will be plotted and set legend labels
	legend_labels = []
	if order is not None:
		new_ys = []
		for catid, catname in order:
			new_ys.append(ys[cats.index(catid)])
			legend_labels.append(catname)
		ys = new_ys
	else:
		legend_labels = cats[:]

	# make the y axis in millions
	reduce_y = lambda y: [yi / 1000000.0 for yi in y]
	ys = [reduce_y(y) for y in ys]

	fig = plt.figure()
	ax = fig.add_subplot(111)

	curves = plot_areas(x, ys, ax)
		
	# display a subfigure id if this is going to be part of a subfigure
	plt.figtext(0.025, 0.025, "a", fontsize=30, weight='bold')

	# display the legends
	legend_str = legend_pos if legend_pos is not None else "upper right"
	cols = legend_cols if legend_cols is not None else 2
	ax.legend(curves, legend_labels, shadow=False, ncol=cols, prop=FontProperties(size=12), numpoints=1, bbox_to_anchor=(.32, .99))

	# x-axis labels:
	# a. pritify
	# b. display every other label
	nice_labels = create_nice_labels(labels)
	xreduced = []
	labels_reduced = []
	count = 0
	for i in range(len(x)):
		count += 1
		if count % 2 == 0:
			xreduced.append(x[i])
			labels_reduced.append(nice_labels[i])

	plt.xticks(xreduced, labels_reduced, rotation=60)

	plt.xlim(x[0], x[-1])

	# set font sizes
	for tick in ax.xaxis.get_major_ticks():
		tick.label.set_fontsize(16)
		
	for tick in ax.yaxis.get_major_ticks():
		tick.label.set_fontsize(22)

	if xlabel is not None:
		ax.set_xlabel(xlabel, fontsize=22)
			
	if ylabel is not None:
		ax.set_ylabel(ylabel, fontsize=22)
	
	if title is not None:
		plt.title(title, fontsize=22)

	plt.grid(True, color="#bbbbbb")

	if inset_result_file is not None:
		inset_ax = plt.axes([.56, .69, .39, .2])
		
		inset_headers, inset_labels, inset_ys = read_results_file(inset_result_file)
		inset_ys = [reduce_y(y) for y in inset_ys]

		if order is not None:
			new_inset_ys = []
			for catid, catname in order:
				new_inset_ys.append(inset_ys[cats.index(catid)])
			inset_ys = new_inset_ys

		inset_curves = plot_areas(range(len(inset_ys[0])), inset_ys, inset_ax)
		inset_ax.axes.get_xaxis().set_visible(False)
		if inset_title is not None:
			inset_ax.set_title(inset_title)

	pp = PdfPages(dest)	
	fig.savefig(pp, format='pdf')
	pp.close()
	plt.close()
	
if __name__ == "__main__":
	line_plot(
		os.path.join(os.getenv("TR"), "volume-level2-month.csv"), 
		os.path.join(os.getenv("TP"), "traffic-volume.pdf"), 
		legend_pos="upper left", legend_cols=1,
		ylabel="Clicks (Millions)",
		order=[('email', 'Mail'), ('social', 'Social Media'), ('search', 'Search')],
		inset_result_file=os.path.join(os.getenv("TR"), "news-level3-month-volume.tab"), inset_title="News"
	)
