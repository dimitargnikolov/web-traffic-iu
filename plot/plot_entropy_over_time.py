import sys, os, argparse, matplotlib, numpy as np, re, math, csv, calendar, numpy

matplotlib.use('Agg')

import matplotlib.pyplot as plt
from matplotlib.backends.backend_pdf import PdfPages
from matplotlib.font_manager import FontProperties
from matplotlib.ticker import MaxNLocator, LinearLocator

from matplotlib import rcParams
rcParams.update({'figure.autolayout': True})

sys.path.append(os.path.join(os.path.dirname(os.path.join(os.getcwd(), __file__)), '..'))
from lib import read_results_file

def fill_missing_values(labels, ys):
	new_labels = []
	new_ys = []
	for _ in range(len(ys)):
		new_ys.append([])
		
	prev = None
	for i in range(len(labels)):
		m = re.search(r'(\d+)\-(\d+)', labels[i])
		assert m is not None
		year, month = int(m.group(1)), int(m.group(2))
		if prev is not None:
			while year == prev[0] and month - prev[1] != 1:
				prev = (prev[0], prev[1] + 1)
				new_labels.append("%d-%02d" % (prev[0], prev[1]))
				for j in range(len(ys)):
					new_ys[j].append(float('NaN'))
			while year - prev[0] == 1 and not (month == 1 and prev[1] == 12):
				prev = (prev[0], prev[1] + 1)
				if prev[1] == 13:
					prev = (prev[0] + 1, 1)
				new_labels.append("%d-%02d" % (prev[0], prev[1]))
				for j in range(len(ys)):
					new_ys[j].append(float('NaN'))
		new_labels.append("%d-%02d" % (year, month))
		for j in range(len(ys)):
			new_ys[j].append(ys[j][i])
		prev = (year, month)

	for i in range(len(new_ys)):
		new_ys[i] = numpy.ma.masked_where(numpy.isnan(new_ys[i]), new_ys[i])
	return new_labels, new_ys
	
def create_nice_labels(labels):
	nice_labels = []
	for label in labels:
		year, month = map(int, re.match("(\d+)\-(\d+)", label).groups())
		nice_labels.append(calendar.month_abbr[month] + " " + str(year)[2:])
	return nice_labels
	
def line_plot(result_file, dest, inset_result_file=None, inset_title=None, order=None, title=None, legend_pos=None, legend_cols=None, xlabel=None, ylabel=None):
	headers, labels, ys = read_results_file(result_file)
	cats = headers[1:]

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

	#labels, ys = fill_missing_values(labels, ys)
	x = range(len(labels))

	fig = plt.figure()
	ax = fig.add_subplot(111)

	colors = ['#fdb863', '#e66101', '#b2abd2', '#5e3c99']
	markers = ['s', 'o', '^', 'x']

	curves = []
	for i in range(len(ys)):
		curve = ax.plot(x, ys[i], linestyle="-", color=colors[i], marker=markers[i], markerfacecolor='none', markeredgecolor=colors[i])[0]
		# to plot the error bars, the error needs to be computed and be place in the same file as the results being plotted
		#ax.errorbar(x, ys[i], errors[i], color=colors[i])
		curves.append(curve)

	legend_str = legend_pos if legend_pos is not None else "upper right"
	cols = legend_cols if legend_cols is not None else 2
	ax.legend(curves, legend_labels, shadow=False, ncol=cols, prop=FontProperties(size=14), numpoints=1, bbox_to_anchor=(0.01, 0.25, 0.30, .2))
	
	# display a subfigure id if this is going to be part of a subfigure
	plt.figtext(0.025, 0.025, "b", fontsize=30, weight='bold')

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
		
	# add an inset to the figure
	if inset_result_file is not None:
		inset_ax = plt.axes([.51, .45, .45, .23])
		
		inset_headers, inset_labels, inset_ys = read_results_file(inset_result_file)
		inset_x = sorted(range(len(inset_labels)))

		# reorder the data that will be plotted and set legend labels
		if order is not None:
			new_inset_ys = []
			for catid, catname in order:
				new_inset_ys.append(inset_ys[cats.index(catid)])
			inset_ys = new_inset_ys

		inset_curves = []
		for i in range(len(inset_ys)):
			curve = inset_ax.plot(inset_x, inset_ys[i], linestyle="-", color=colors[i], marker=markers[i], markerfacecolor='none', markeredgecolor=colors[i])[0]
			inset_curves.append(curve)
		inset_ax.axes.get_xaxis().set_visible(False)

		if inset_title is not None:
			inset_ax.set_title(inset_title)

	pp = PdfPages(dest)	
	fig.savefig(pp, format='pdf')
	pp.close()
	plt.close()
	
if __name__ == "__main__":
	line_plot(
		os.path.join(os.getenv("TR"), "smooth-level2-month-entropy.txt"), 
		os.path.join(os.getenv("TP"), "entropy-over-time.pdf"), 
		legend_pos="center right", legend_cols=1,
		ylabel="Entropy",
		order=[('email', 'Mail'), ('social', 'Social Media'), ('search', 'Search')],
		inset_result_file=os.path.join(os.getenv("TR"), "smooth-news-full-month-entropy.txt"), inset_title="News"
	)

