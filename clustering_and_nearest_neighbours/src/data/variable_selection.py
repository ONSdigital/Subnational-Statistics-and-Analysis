from typing import Dict
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.preprocessing import StandardScaler, MinMaxScaler
from sklearn.metrics import pairwise_distances
from sklearn.decomposition import PCA


def get_correlation_matrix(
    df: pd.DataFrame,
) -> pd.DataFrame:
    """
    

    Parameters
    ----------
    df : pd.DataFrame
        The data to be fed into the clustering model.

    Returns
    -------
    df_corr : pd.dataframe
        A correlation matrix for all selected variables

    """
    df = df.set_index("AREACD")
    df_corr = df.corr()
    return df_corr

def pca_analysis(
        loaded_config: Dict,
        df:pd.DataFrame,
        Area_code: str="AREACD",
        threshold: float=0.25,
        ):
        """
    Perform PCA analysis on the given DataFrame and generate plots and dataframes for explained variance and feature loadings.

    Parameters:
    loaded_config (Dict): Configuration dictionary containing the output file path.
    df (pd.DataFrame): DataFrame containing the data to be analyzed.
    Area_code (str, optional): Column name to be used as the index for the DataFrame. Default is "AREACD".
    threshold (float, optional): Threshold for selecting important features based on their loadings. Default is 0.25.

    Returns:
    pcs (pd.DataFrame): DataFrame containing the principal components and their explained variance ratios.
    loading_df (pd.DataFrame): DataFrame containing the loadings of each feature for each principal component.
    important_features (pd.DataFrame): DataFrame containing the features with loadings above the specified threshold.
        """
        path=loaded_config["outputs_file_path"]
        df = df.set_index(Area_code)
        df.fillna(0, inplace=True)
        scaler = StandardScaler()
        df_scaled = scaler.fit_transform(df)
        
        pca = PCA()
        pca.fit(df_scaled)
        
        explained_variance = pca.explained_variance_ratio_
        
        plt.figure(figsize=(15, 6))
        plt.plot(range(1, len(explained_variance) + 1), explained_variance, marker='o')
        plt.title('Explained Variance by Principal Components')
        plt.xlabel('Principal Component')
        plt.ylabel('Explained Variance Ratio')
        plt.xticks(range(1, len(explained_variance) + 1))
        plt.grid()
        plt.savefig(f"{path}/Explained Variance by Principal Components.jpeg")
        pcs = pd.DataFrame(range(1, len(explained_variance) + 1), explained_variance)
        
        loadings = pca.components_.T
        loading_df = pd.DataFrame(loadings, index=df.columns, columns=[f'PC{i+1}' for i in range(loadings.shape[1])])
        
        important_features = loading_df[loading_df.abs() > threshold]
        
        return pcs, loading_df, important_features
    
    
def variance_analysis(
        loaded_config: Dict,
        df:pd.DataFrame,
        Area_code: str="AREACD",
                      ):
        """
    Perform variance analysis on the given DataFrame and generate a plot for the variance of Min-Max scaled features.

    Parameters:
    loaded_config (Dict): Configuration dictionary containing the output file path.
    df (pd.DataFrame): DataFrame containing the data to be analyzed.
    Area_code (str, optional): Column name to be dropped from the DataFrame. Default is "AREACD".

    Returns:
    variance (pd.Series): Series containing the variance of each feature after Min-Max scaling.
    """
        path=loaded_config["outputs_file_path"]
        df = df.drop(Area_code, axis=1)
        scaler = MinMaxScaler()
        df_minmax_scaled = pd.DataFrame(scaler.fit_transform(df), columns=df.columns)
        variance = df_minmax_scaled.var()
        plt.figure(figsize=(10, 8))
        variance.plot(kind='barh', color='skyblue')
        plt.title('Variance of Min_max Scaled Features')
        plt.xlabel('Variance')
        plt.ylabel('Features')
        plt.savefig(f"{path}/Variance of Min_max Scaled Features.jpeg")
        return variance

def visualize_pairwise_distances(
        loaded_config: Dict,
        df:pd.DataFrame,
        Area_code: str="AREACD",
                      ):
        """
    Calculate and visualize pairwise distances between rows in the given DataFrame.

    Parameters:
    loaded_config (Dict): Configuration dictionary containing the output file path.
    df (pd.DataFrame): DataFrame containing the data to be analyzed.
    Area_code (str, optional): Column name to be used as the index for the DataFrame. Default is "AREACD".

    Returns:
    distances (np.ndarray): Array containing the pairwise distances between rows of the scaled DataFrame.
    """
        path=loaded_config["outputs_file_path"]
        df = df.set_index(Area_code)
        df.fillna(0, inplace=True)
        scaler = StandardScaler()
        df_scaled = scaler.fit_transform(df)
        distances = pairwise_distances(df_scaled, metric='euclidean')
        plt.figure(figsize=(10, 6))
        plt.hist(distances.flatten(), bins=50, color='blue', alpha=0.7)
        plt.title('Histogram of Pairwise Distances')
        plt.xlabel('Distance')
        plt.ylabel('Frequency')
        plt.savefig(f"{path}/Histogram of Pairwise Distances.jpeg")
        return distances

