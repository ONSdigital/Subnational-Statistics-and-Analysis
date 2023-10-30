def metrics_to_table(metrics):
    import pandas as pd
    metrics = pd.concat(metrics)
    cols = metrics['Indicator'].drop_duplicates()
    metrics = pd.pivot_table(metrics, values='Value', columns='Indicator', index='AREACD')[cols].reindex()
    return(metrics)

def make_clustering_model(n_clusters=5, algorithm='kmeans'):
    """This will define a clustering model. Defaults to k-means with 5 clusters. Advantage of this initialisation is seed fixing for reproducbility.
    """
    #As we're using non-determintic process, set the seed for reproducibility
    import numpy as np
    np.random.seed(seed=19042022)
    #if algorithm=='kmeans' #In principle, could tweak this, but only Kmeans implemented so commented out.
    from sklearn.cluster import KMeans
    kmeans = KMeans(n_clusters=n_clusters, n_init=10, max_iter=300)
    return(kmeans)

#For stuff which wants to do maps of the clusters, need to import the LA shapefile
def get_la_shapefile():
    if 'la_geo' not in globals(): #If we haven't got this read in, read from bigquery.
        from google.cloud import bigquery
        import pandas as pd    
        try: 
            import geopandas
        except: 
            import pip
            pip.main(['install', geopandas])
            import geopandas
        client = bigquery.Client(location="location")

        query = """SELECT LAD20CD, geom, BNG_E, BNG_N
        FROM `project.geography_ingest_dataset_name.geography_ingest_table_name`
        """
        query_job = client.query(query, location="location",)
        global la_geo
        la_geo = query_job.to_geodataframe
        return(la_geo)
    else: #If we already have, just return it.
        return(la_geo)

def get_clusters(metrics, model):
    from sklearn.preprocessing import StandardScaler
    import pandas as pd
    no_na_metrics = metrics[metrics.notna().all(axis=1)]
    scaler = StandardScaler()
    metrics_scaled = scaler.fit_transform(no_na_metrics)
    model.fit(metrics_scaled)
    clusters = pd.DataFrame(no_na_metrics.reset_index()['AREACD'])
    clusters['Cluster'] = model.labels_
    la_geo = get_la_shapefile()
    clusters = la_geo().merge(clusters, right_on = 'AREACD', left_on = 'LAD20CD', how='right')
    return(clusters, model)

def clusters_and_map(metrics, model, cmap="Spectral", show_plots=True, optimise=True):
    if optimise:
        clusters, model = get_best_clusters(metrics, model)
    else:
        clusters, model = get_clusters(metrics, model)
    if show_plots:
        clusters.plot(column = "Cluster", cmap=cmap)
    return(clusters)


def clusters_and_plots(metrics, model, x=0, y=1, cmap="Spectral", show_plots=True, optimise=True):
    import numpy as np
    import matplotlib.pyplot as plt
    if optimise:
        clusters, model = get_best_clusters(metrics, model)
    else:
        clusters, model = get_clusters(metrics, model)
    analysis = clusters.merge(metrics, on="AREACD")
    if show_plots:
        #Do radar plot.
        centres = model.cluster_centers_
        categories = metrics.columns
        categories = [*categories, categories[0]]
        cluster_centres=[]
        for i in range(len(centres)):
            cluster_i = centres[i].tolist()
            cluster_i = [*cluster_i, cluster_i[0]]
            cluster_centres.append(cluster_i)
        #Need to get the cluster ranking and rearrange the list of centres for this plot.
        ranks = relabel_clusters(clusters, model, output='ranks')
        cluster_centres = [cluster_centres[rank] for rank in ranks]

        label_loc = np.linspace(start=0, stop=2 * np.pi, num=len(categories))
        plt.figure(figsize=(8, 8))
        plt.subplot(polar=True)
        for i in range(len(centres)):
            plt.plot(label_loc, cluster_centres[i], label='Cluster'+str(i))
        plt.title('Radar plot', size=20, y=1.05)
        lines, labels = plt.thetagrids(np.degrees(label_loc), labels=categories)
        plt.legend()
        plt.show()
        
        n= len(centres)
        analysis.plot.scatter(x=list(metrics)[x], 
                               y=list(metrics)[y],
                               c="Cluster", cmap="tab10", vmin=0, vmax=10)
        analysis.plot(column = "Cluster", cmap="tab10", vmin=0, vmax=10)
    return(analysis)

def clusters_and_plots_cb(metrics, model, x=0, y=1, cmap="ons_colours", show_plots=True, optimise=True):
    import numpy as np
    import matplotlib.pyplot as plt
    import matplotlib as mpl
    if optimise:
        clusters, model = get_best_clusters(metrics, model)
    else:
        clusters, model = get_clusters(metrics, model)
    analysis = clusters.merge(metrics, on="AREACD")
    if show_plots:
        #ONS Colour Palette
        from matplotlib.colors import ListedColormap
        import seaborn as sns
        sns.color_palette()
        ons = ['#12436D', '#28A197', '#801650', '#F46A25', '#3D3D3D', '#A285D1', '#ffdd00', '#d4351c', '#f499be', '#5694ca']
        ons_colours = ListedColormap(sns.color_palette(ons))
        
        #Do radar plot.
        centres = model.cluster_centers_
        categories = metrics.columns
        categories = [*categories, categories[0]]
        cluster_centres=[]
        for i in range(len(centres)):
            cluster_i = centres[i].tolist()
            cluster_i = [*cluster_i, cluster_i[0]]
            cluster_centres.append(cluster_i)
        #Need to get the cluster ranking and rearrange the list of centres for this plot.
        ranks = relabel_clusters(clusters, model, output='ranks')
        cluster_centres = [cluster_centres[rank] for rank in ranks]

        label_loc = np.linspace(start=0, stop=2 * np.pi, num=len(categories))
        plt.figure(figsize=(8, 8))
        plt.subplot(polar=True)
        for i in range(len(centres)):
            plt.plot(label_loc, cluster_centres[i], c = ons[i], label='Cluster'+str(i))
        plt.title('Radar plot', size=20, y=1.05)
        lines, labels = plt.thetagrids(np.degrees(label_loc), labels=categories)
        plt.legend()
        plt.show()
                
        n= len(centres)
        analysis.plot.scatter(x=list(metrics)[x], 
                               y=list(metrics)[y],
                               c="Cluster", cmap=ons_colours, vmin=0, vmax=10) #cmap used to be table10
        analysis.plot(column = "Cluster", cmap=ons_colours, vmin=0, vmax=10)
    return(analysis)

#To assess performance of clusters, use the silhouette
from sklearn.metrics import silhouette_samples, silhouette_score
def get_silhouette_metric(clusters, model):
    cols = list(clusters)
    left = cols.index('Cluster')+1
    right = len(cols)
    metrics = clusters.iloc[:, left:right]
    labels = model.fit_predict(metrics)
    sil_score = silhouette_score(metrics, labels)
    return(sil_score)

def optimise_k(metrics, model, min_k=4, max_k=15):
    '''Enter metrics in wide (pviot table) format -- use metrics_to_table if needed.'''
    best_k = 0
    best_sil = 0
    for k in range(min_k, max_k):
        model = make_clustering_model(n_clusters=k)
        #WARNING: For scanning through values of k, need to used the unoptimised version of this function!
        #Otherwise, it'll get stuck in a recursion loop.
        clusters_k = clusters_and_plots(metrics, model, show_plots=False, optimise=False)
        sil = get_silhouette_metric(clusters_k, model)
        if sil > best_sil:
            best_sil = sil
            best_k = k
    return(best_k)

#A function to relabel clusters based on performance.
#Assume that performance is measured by value of the 0th dimension in the cluster.
#THAT IS: we put the metrics in with the headline that we want to use for performance first in the list.
def relabel_clusters(clusters, model, output='clusters'):
    '''USAGE OPTIONS -- we can use the optional argument output='ranks' to return the cluster ranking.'''
    centres = model.cluster_centers_
    import pandas as pd
    performance = list(pd.DataFrame(centres)[0])
    #Get the rank of each. This will give the ranking
    indices = list(range(len(performance)))
    indices.sort(key=lambda x: performance[x])
    ranking = [0] * len(indices)
    for i, x in enumerate(indices):
        ranking[x] = i
    #Return just the ranking if requested
    if output=='ranks':
        return(indices)
    #Usually, relabel the clusters and return those
    else:
        #Then relabel based on this ranking vector.
        clusters['Cluster'] = clusters['Cluster'].apply(lambda x: ranking[x])
        return(clusters)

#This will optimise over k and relabel clusters.
def get_best_clusters(metrics, model):
    n_clusters = optimise_k(metrics, model)
    best_model = make_clustering_model(n_clusters=n_clusters)
    clusters, model = get_clusters(metrics, best_model)
    clusters = relabel_clusters(clusters, best_model)
    return(clusters, model)

#File for LA codes and LA names for output tables
def get_la_names_LAD20():
    if 'la_names' not in globals(): #If we haven't got this read in, read from bigquery.
        from google.cloud import bigquery
        client = bigquery.Client(location="location")

        query = """SELECT LAD20CD, LAD20NM
        FROM `project.geography_ingest_dataset_name.local_authority_table_name`
        """
        query_job = client.query(query, location="location",)
        la_names = query_job.to_dataframe()
        return(la_names)
    else: #If we already have, just return it.
        return(la_names)
    
def get_la_names():
    if 'la_names' not in globals(): #If we haven't got this read in, read from bigquery.
        from google.cloud import bigquery
        client = bigquery.Client(location="location")

        query = """SELECT AREA_CODE, LOCAL_AREA_NAME
        FROM `project.ingest_dataset_name.local_authority_lap_table_name`
        """
        query_job = client.query(query, location="location",)
        la_names = query_job.to_dataframe()
        return(la_names)
    else: #If we already have, just return it.
        return(la_names)

def get_la_names_LAD23():
    if 'la_names_LAD23' not in globals(): #If we haven't got this read in, read from bigquery.
        from google.cloud import bigquery
        client = bigquery.Client(location=" location")

        query = """SELECT LAD23CD, LAD23NM
        FROM `project.ingest_dataset_name.local_authority_lap_table_name_23`
        """
        query_job = client.query(query, location="europe-west2",)
        la_names_LAD23 = query_job.to_dataframe()
        la_names_LAD23.rename(columns = {'LAD23CD':'AREA_CODE'}, inplace = True)
        la_names_LAD23.rename(columns = {'LAD23NM':'LOCAL_AREA_NAME'}, inplace = True)
       
        return(la_names_LAD23)
    else: #If we already have, just return it.
        return(la_names_LAD23)

#Produces output table for cluster model (alternative to maps)
#Older function for LAD20 boundaries
def clusters_and_table_LAD20(metrics, model, optimise=True):
    if optimise:
        clusters, model = get_best_clusters(metrics, model)
        la_names = get_la_names()
        clusters = la_names.merge(clusters, on = 'LAD20CD')
    else:
        clusters, model = get_clusters(metrics, model)
        la_names = get_la_names()
        clusters = la_names.merge(clusters, on = 'LAD20CD')
    return(clusters.sort_values(by=['LAD20CD']).filter(items=['LAD20CD', 'LAD20NM', 'Cluster']))

def clusters_and_table(metrics, model, optimise=True):
    import numpy as np
    import pandas as pd
    n_clusters = optimise_k(metrics, model)
    best_model = make_clustering_model(n_clusters=n_clusters)
    clusters, model = get_clusters(metrics, best_model)
    clusters = relabel_clusters(clusters, best_model)
    la_names = get_la_names()
    clusters = la_names.merge(clusters, right_on = 'AREACD', left_on = 'AREA_CODE', how='right')
    clusters['LOCAL_AREA_NAME'] = np.where(clusters['AREACD']=='E06000060', 'Buckinghamshire', clusters['LOCAL_AREA_NAME'])
    clusters['LOCAL_AREA_NAME'] = np.where(clusters['AREACD']=='E06000061', 'North Northamptonshire', clusters['LOCAL_AREA_NAME'])
    clusters = clusters.dropna(axis=0, subset=['LOCAL_AREA_NAME'])
    return(clusters.sort_values(by=['AREACD']).filter(items=['AREACD', 'LOCAL_AREA_NAME', 'Cluster']))

def clusters_and_table_LAD23(metrics, model, optimise=True):
    import numpy as np
    import pandas as pd
    n_clusters = optimise_k(metrics, model)
    best_model = make_clustering_model(n_clusters=n_clusters)
    clusters, model = get_clusters(metrics, best_model)
    clusters = relabel_clusters(clusters, best_model)
    la_names = get_la_names_LAD23()
    clusters = la_names.merge(clusters, right_on = 'AREACD', left_on = 'AREA_CODE', how='right')
    clusters['LOCAL_AREA_NAME'] = np.where(clusters['AREACD']=='E06000060', 'Buckinghamshire', clusters['LOCAL_AREA_NAME'])
    clusters['LOCAL_AREA_NAME'] = np.where(clusters['AREACD']=='E06000061', 'North Northamptonshire', clusters['LOCAL_AREA_NAME'])
    clusters = clusters.dropna(axis=0, subset=['LOCAL_AREA_NAME'])
    return(clusters.sort_values(by=['AREACD']).filter(items=['AREACD', 'LOCAL_AREA_NAME', 'Cluster']))

def clusters_mean(table_metrics, clusters_table):
    table_metrics_rescale = table_metrics.rename_axis(None).reset_index(level=0)
    table_metrics_rescale = table_metrics_rescale.rename(columns = {'index': 'AREACD'}, inplace = False)
    clusters_head_figures = clusters_table.merge(table_metrics_rescale, on = "AREACD", how = "left")
    clusters_head_avg = clusters_head_figures.groupby(['Cluster']).agg(['mean'])
    return(clusters_head_avg)

def clusters_median(table_metrics, clusters_table):
    table_metrics_rescale = table_metrics.rename_axis(None).reset_index(level=0)
    table_metrics_rescale = table_metrics_rescale.rename(columns = {'index': 'AREACD'}, inplace = False)
    clusters_head_figures = clusters_table.merge(table_metrics_rescale, on = "AREACD", how = "left")
    clusters_head_avg = clusters_head_figures.groupby(['Cluster']).agg(['median'])
    return(clusters_head_avg)

def clusters_mean_overall(table_metrics, clusters_table):
    table_metrics_rescale = table_metrics.rename_axis(None).reset_index(level=0)
    table_metrics_rescale = table_metrics_rescale.rename(columns = {'index': 'AREACD'}, inplace = False)
    clusters_head_figures = clusters_table.merge(table_metrics_rescale, on = "AREACD", how = "left")
    clusters_head_avg = clusters_head_figures.agg(['median', 'mean'])
    return(clusters_head_avg.drop(columns = ['Cluster']))