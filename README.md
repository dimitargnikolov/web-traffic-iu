Preliminaries
   1. Make sure that the following environmental variables are created on your machine: TC, TR, TP, TD (short for Traffic Code, Traffic Results, Traffic Plots and Traffic Data, respectively).

   For example, one way to do this is to put the following lines in your .bashrc file.

       export TBASE="/l/nx/data/click/web-traffic"
       export TC="$TBASE/code"
       export TR="$TBASE/results"
       export TP="$TBASE/plots"
       export TD="$TBASE/data"

   2. If you want to reproduce the full set of experiments:
      2.1. Go to http://cnets.indiana.edu/groups/nan/webtraffic/click-dataset/ and request the full dataset of web requests.
      2.2. Once you have the data, start from step I.

   3. If you want to reproduce only the analysis without doing all the data filtering from scratch:
      3.1. Start from the filtered dataset distributed with this README and go to step IV in this README.
      3.2. If for whatever reason, you don't have the filtered dataset, go to Harvard Dataverse and download it from .

I. Data preparation: granularity & convert to 3rd level domains.
   1. Create visit matrix files from the human requests click files: create_vm.py
   2. Change the granularity of the vm files from hour to day, from day to week and from day to month: 
      2.1. transform/change_gran_hour_to_day.py 
      2.2. transform/change_gran_day_to_week.py
      2.3. transform/change_gran_day_to_month.py
   3. Create a dataset with maximum domain level of 3: transform/change_domain_level_full_to_3.py

II. Data filtering: remove ads, auto, iu and intra category traffic, filter referrers to be in the categories of interest.
   1. Remove traffic not coming from the categories of interest: filter/filter_referrers.py
   2. Remove traffic going to the categories of interest: filter/filter_targets.py
   3. Remove IU traffic: filter/remove_iu.py
   4. Remove advertising and data serving traffic: filter/remove_unwanted.py
   5. You can use filter/remove_adhoc.py to remove more domains as necessary. Make sure you update the list in filter/remove_unwanted.py with what is in filter/remove_adhoc.py if appropriate.

III. More data preparation: convert to 2nd level domains and create a separate dataset for each category.
   1. Split the vms into categories for empty, search, social media, mail: transform/create_category_vms.py
   2. Create level2 domain datasets from the category data: transform/change_domain_level_3_to_2.py
   
IV. Traffic volume
    1. Compute the traffic volumes for each category in the entire dataset: analyze/compute_traffic_volume.py
    2. Plot the traffic volume: plot/plot_traffic_volume.py

V. Overall entropy
    1. Using the results from the traffic volume computation, determine how many visits to sample from all time periods. In order not to lose any data points, we need to sample the lowest number of clicks for the lowest month, for the lowest category. E.g. if the lowest number of clicks ever is wikipedia in Sep 2006, so take that number.
    2. Run the sampling script passing it the number obtained above: filter/create_over_time_samples.py
       2.1. create_over_time_samples.py creates samples for each month. An alternative way to do this is to run filter/create_samples.py, which samples the *full* dataset multiple times. In the first case, the results have a temporal component, in the second, they don't.
    3. Compute the entropy of the newly sampled dataset: analyze/compute_entropy.py
    4. Plot the entropy that was just computed: plot/plot_entropy.py
 
VI. Entropy over time
    1. Using the results from the traffic volume computation, determine how many visits to sample per time period. 
    2. Run the sampling script passing it the number obtained above: filter/create_over_time_sample.py
    3. Create smoothing datasets for the different categories: transform/smooth_vms.py
    4. Compute the entropy of the newly smoothed dataset: analyze/compute_smooth_entropy.py
    5. Plot the entropy that was just compute: plot/plot_entropy_over_time.py

VII. Entropy vs traffic volume computations
   1. Create random datasets with increasing numbers of requests: transform/create_volume_vs_entropy_dataset.py
   2. Compute the entropy versus traffic volume results: analyze/compute_entropy_vs_traffic_volume.py
   3. Plot the entropy and traffic volume just computed: plot/plot_entropy_vs_traffic_volume.py

VIII. Twitter and AOL
   n. scatter_plot.py: Plot user vs collective entropy for Twitter and AOL.

IX. News
    1. Run scrapy to crawl the news sources from dmoz.org: in news/ run 'scrapy crawl NewsLinksSpider'
    2. Filter the news urls: news/filter_news_urls.py
    3. Filter the full-domain data by url: filter/filter_non_news.py
    4. Run the pipeline starting from II but don't change the domain level to 3 or 2. In addition, at the end of step II, run 'filter/filter_news_junk.py'.
