# Preliminaries
* This is the code to reproduce the experiments in [Dimitar G Nikolov, Diego F. M. Oliveira, Alessandro Flammini, Filippo Menczer. Measuring Online Social Bubbles, 2015](http://arxiv.org/abs/1502.07162).
* All scripts run on Python 2.7. If you do not have Python set up, the easiest way to do it and ensure you have the necessary libraries to run the code, is to download the [Python 2.7 Anaconda distribution](https://www.continuum.io/downloads).
* The first thing you need to do is go to http://cnets.indiana.edu/groups/nan/webtraffic/click-dataset/ and request the full dataset of web requests. The data is public, but not open, meaning that you will need to submit a request for it that will go through a review. This is necessary due to sensitivity of the data, and us wanting to be confident it will be used for legitimate research purposes.
* Once you have the data, you can follow the steps below to process it and generate the various plots from the paper. The scripts are designed to be self-contained and run with minimal changes, but you may need to change a few paths around when you carry out the news targets data preparation. In addition, most of the scripts are designed to run in parallel on 16 cores. Changing the parallelism level is per script and should be pretty easy to do when you look at the code.
* Before you begin, make sure that the following environmental variables are created on your machine: TC, TR, TP, TD (short for Traffic Code, Traffic Results, Traffic Plots and Traffic Data, respectively). For example, one way to do this is to put the following lines in your .bashrc file.
```
export TBASE="/home/you/projects/web-traffic"
export TC="$TBASE/code"
export TR="$TBASE/results"
export TP="$TBASE/plots"
export TD="$TBASE/data"
```

You can follow the following data path in running the experiments.

# Data Preparation

Change the granularity of the data (hour to day, day to month) and conver to 3rd level domains.

1. Get rid of invalid requests and transform the valid ones to a form easier to parse later: validate_requests.py
2. Create visit matrix files from the human requests click files: create_vm.py
3. Change the granularity of the vm files from hour to day and from day to month: 
  1. transform/change_gran_hour_to_day.py 
  2. transform/change_gran_day_to_month.py
4. Create a dataset with maximum domain level of 3: transform/change_domain_level_full_to_3.py

# Data Filtering

Remove ads, auto, iu and intra category traffic, filter referrers to be in the categories of interest, etc.

1. Remove traffic not coming from the categories of interest: filter/filter_referrers.py
2. Remove traffic going to the categories of interest: filter/filter_targets.py
3. Remove IU traffic: filter/remove_iu.py
4. Remove advertising and data serving traffic: filter/remove_unwanted.py
5. Make the following symbolic link to make things easier: ln -s $TD/vm/level3-domain/month/filtered-referrers--filtered-targets--no-iu--no-unwanted $TD/vm/level3-domain/month/filtered

# More Data Preparation

Convert to 2nd level domains and create a separate dataset for each category.

1. Split the vms into categories for search, social media, mail: transform/create_category_vms.py
2. Create level2 domain datasets from the category data: transform/change_domain_level_3_to_2.py

# Preparing News-Only Data

1. Run scrapy to crawl the news sources from dmoz.org: in *$TC/news* run 'scrapy crawl NewsLinksSpider'
2. Filter the news urls: news/filter_news_urls.py
3. Filter the full-domain data by url: filter/filter_non_news.py
4. Run these scripts from the filtering pipeline on the news-only data: filter_referrers.py, filter_targets.py, remove_iu.py, remove_unwanted.py.
5. Run filter/filter_news_junk.py to filter some of the likely non-human news traffic.
6. Make a link to the filtered data: ln -s $TD/vm/news/full-domain/month/filtered-referrers--filtered-targets--no-iu--no-unwanted--no-junk $TD/vm/news/full-domain/month/filtered
7. Split up the dataset by category: transform/create_category_vms.py

# Traffic Volume Analysis

1. Compute the traffic volumes for each category in the entire dataset: analyze/compute_traffic_volume.py. Run for both news and all targets.
2. Plot the traffic volume: plot/plot_traffic_volume.py

# Overall Entropy Analysis

1. Using the results from the traffic volume computation, determine how many visits to sample from all time periods. Used in the analysis were 1,000 for news-only targets and 80,000 for all targets.
2. Run the sampling script passing it the number obtained above: filter/create_over_time_samples.py
  1. create_over_time_samples.py creates samples for each month. An alternative way to do this is to run filter/create_samples.py, which samples the *full* dataset multiple times. In the first case, the results have a temporal component, in the second, they don't.
3. Compute the entropy of the newly sampled dataset: analyze/compute_entropy.py
4. Plot the entropy that was just computed: plot/plot_entropy.py
 
# Entropy Over Time Analysis

0. Note that steps 1 and 2 were probably already completed.
1. Using the results from the traffic volume computation, determine how many visits to sample per time period. 
2. Run the sampling script passing it the number obtained above: filter/create_over_time_sample.py
3. Create smoothing datasets for the different categories: transform/smooth_vms.py
4. Compute the entropy of the newly smoothed dataset: analyze/compute_entropy.py
5. Plot the entropy that was just compute: plot/plot_entropy_over_timle.py

# Entropy vs Traffic Volume Analysis

1. Create random datasets with increasing numbers of requests: transform/create_volume_vs_entropy_dataset.py
2. Compute the entropy versus traffic volume results: analyze/compute_entropy_vs_traffic_volume.py
3. Plot the entropy and traffic volume just computed: plot/plot_entropy_vs_traffic_volume.py

# Twitter and AOL Computations

See README in *twitter* directory.
