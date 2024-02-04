
from cProfile import label
from dis import dis
from re import A
from statistics import median, quantiles
#from turtle import color
from matplotlib import markers
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import seaborn as sns
from matplotlib.patches import Patch
from matplotlib.backends.backend_pdf import PdfPages
import statsmodels.graphics.boxplots as smg

# key-value lines plot
def key_value_reads():
    params = {'legend.fontsize': 12,
              'figure.figsize': (16, 10),
              'axes.labelsize': 'x-large',
              'axes.titlesize': 'x-large',
              'xtick.labelsize': '14',
              'ytick.labelsize': '14'}
    plt.rcParams.update(params)

    hlf_kv = pd.read_excel('../data/data-final/experiments-delta-hlf-mongo.ods', sheet_name='DLT-KV-query',
        header=0)
    quo_kv = pd.read_excel('../data/data-final/experiments-delta-quorum-mongo.ods', sheet_name='DLT-KV-query',
        header=0)
    db_kv = pd.read_excel('../data/delta-experiments/database.ods', sheet_name='MongoDB-KV-query',
        header=0)
    pg_kv = pd.read_excel('../data/delta-experiments/database.ods', sheet_name='PostgreSQL-KV-query',
        header=0)

    key_value_reads_df = pd.concat([hlf_kv["SmallAsset-DLT"], quo_kv["SmallAsset-DLT"], db_kv["SmallAsset-Mongo"], pg_kv["SmallAsset-PSQL"],
                                    hlf_kv["IntermAsset-DLT"],quo_kv["IntermAsset-DLT"],db_kv["IntermediateAsset-Mongo"],pg_kv["IntermediateAsset-PSQL"],
                                    hlf_kv["LargeAsset-DLT"],quo_kv["LargeAsset-DLT"],db_kv["LargeAsset-Mongo"],pg_kv["LargeAsset-PSQL"]],
                                    keys=['Small asset - HLF', 'Small asset - Quorum', 'Small asset - MongoDB', 'Small asset - PostgreSQL',
                                          'Medium asset - HLF','Medium asset - Quorum', 'Medium asset - MongoDB',  'Medium asset - PostgreSQL',
                                          'Large asset - HLF','Large asset - Quorum', 'Large asset - MongoDB', 'Large asset - PostgreSQL'],
                                    axis=1, ignore_index=False, copy=False)

    #x_coords = key_value_reads_df.index
    key_value_reads_df.index = [1000,2000,3000,4000,5000,8000,14000,20000,30000,40000]
    axis = key_value_reads_df.plot(xlim=[0, 41000], logy=False,
                                   color=(['tab:red', 'tab:blue', 'tab:purple', 'tab:green',
                                            'tab:red', 'tab:blue', 'tab:purple', 'tab:green',
                                            'tab:red', 'tab:blue', 'tab:purple', 'tab:green',
                                            'tab:red', 'tab:blue', 'tab:purple', 'tab:green',]),
                                   style='-', marker='s',figsize=(13,10))
    markers = (['s','s','s','s','D','D','D','D','o','o','o','o'])
    for i, line in enumerate(axis.get_lines()):
        line.set_marker(markers[i])
        line.set_linewidth(1)
    axis.set_yscale("log")
    plt.ylim([0, 1e+04])
    plt.legend(shadow=False,ncol=3,fontsize=12)
    axis.legend(shadow=True, loc='upper right', bbox_to_anchor=(1.0, 1.0), ncol=3)
    axis.set_ylabel('Read time [ms]', labelpad=18)
    axis.set_xlabel('Stored data [K-assets]', labelpad=18)
    axis.set_xticks([1000, 8000, 14000, 20000, 30000, 40000], [1, 8, 14, 20, 30, 40])
    plt.subplots_adjust(top=0.85)
    plt.savefig('kv_lineas.svg', format='svg')

    with PdfPages('key_value_reads.pdf') as pdf:
        pdf.savefig()

# full violin plot
def plot_beans_complex():
    large = 22; med = 16; small = 12
    params = {'axes.titlesize': large,
          'legend.fontsize': med,
          'figure.figsize': (16, 10),
          'axes.labelsize': med,
          'axes.titlesize': med,
          'xtick.labelsize': med,
          'ytick.labelsize': small,
          'figure.titlesize': large}
    plt.rcParams.update(params)
    #plt.style.use('seaborn-whitegrid')
    sns.set_style("white")

    quorum_simple = pd.read_excel('../data/violin/query-median-2-8-24-40.ods', sheet_name='QUORUM-SMALL-COMPLEX',
        header=0, decimal=',')
    quorum_medium = pd.read_excel('../data/violin/query-median-2-8-24-40.ods', sheet_name='QUORUM-INTERMEDIATE-COMPLEX',
        header=0, decimal=',')
    quorum_complex = pd.read_excel('../data/violin/query-median-2-8-24-40.ods', sheet_name='QUORUM-LARGE-COMPLEX', 
        header=0, decimal=',')

    fabric_simple = pd.read_excel('../data/violin/query-median-2-8-24-40.ods', sheet_name='FABRIC-SMALL-COMPLEX', 
        header=0, decimal=',')
    fabric_medium = pd.read_excel('../data/violin/query-median-2-8-24-40.ods', sheet_name='FABRIC-INTERMEDIATE-COMPLEX', 
        header=0, decimal=',')
    fabric_complex = pd.read_excel('../data/violin/query-median-2-8-24-40.ods', sheet_name='FABRIC-LARGE-COMPLEX', 
        header=0, decimal=',')

    fabric_simple_rich = pd.read_excel('../data/violin/query-median-2-8-24-40.ods', sheet_name='FABRIC-SMALL-RICH', 
        header=0, decimal=',')
    fabric_medium_rich = pd.read_excel('../data/violin/query-median-2-8-24-40.ods', sheet_name='FABRIC-INTERMEDIATE-RICH', 
        header=0, decimal=',')
    fabric_complex_rich = pd.read_excel('../data/violin/query-median-2-8-24-40.ods', sheet_name='FABRIC-LARGE-RICH', 
        header=0, decimal=',')

    mongo_simple = pd.read_excel('../data/violin/query-median-2-8-24-40.ods', sheet_name='MONGO-SMALL-COMPLEX', 
        header=0, decimal=',')
    mongo_medium = pd.read_excel('../data/violin/query-median-2-8-24-40.ods', sheet_name='MONGO-INTERMEDIATE-COMPLEX', 
        header=0, decimal=',')
    mongo_complex = pd.read_excel('../data/violin/query-median-2-8-24-40.ods', sheet_name='MONGO-LARGE-COMPLEX', 
        header=0, decimal=',')

    postgres_simple = pd.read_excel('../data/delta-experiments/query-median-db-2-8-24-40.ods', sheet_name='PSQL-SMALL-COMPLEX', 
        header=0, decimal=',')
    postgres_medium = pd.read_excel('../data/delta-experiments/query-median-db-2-8-24-40.ods', sheet_name='PSQL-INTERMEDIATE-COMPLEX', 
        header=0, decimal=',')
    postgres_complex = pd.read_excel('../data/delta-experiments/query-median-db-2-8-24-40.ods', sheet_name='PSQL-LARGE-COMPLEX', 
        header=0, decimal=',')
    
    df = pd.concat([fabric_simple.iloc[:,0], fabric_simple_rich.iloc[:,0],mongo_simple.iloc[:,0], postgres_simple.iloc[:,0], 
                    fabric_simple.iloc[:,2], fabric_simple_rich.iloc[:,2],mongo_simple.iloc[:,2], postgres_simple.iloc[:,2],
                    fabric_simple.iloc[:,3], fabric_simple_rich.iloc[:,3],mongo_simple.iloc[:,3], postgres_simple.iloc[:,3],
                    fabric_medium.iloc[:,0], fabric_medium_rich.iloc[:,0],mongo_medium.iloc[:,0], postgres_medium.iloc[:,0],
                    fabric_medium.iloc[:,2], fabric_medium_rich.iloc[:,2],mongo_medium.iloc[:,2], postgres_medium.iloc[:,2],
                    fabric_medium.iloc[:,3], fabric_medium_rich.iloc[:,3],mongo_medium.iloc[:,3], postgres_medium.iloc[:,3],
                    fabric_complex.iloc[:,0], fabric_complex_rich.iloc[:,0],mongo_complex.iloc[:,0], postgres_complex.iloc[:,0],
                    fabric_complex.iloc[:,2], fabric_complex_rich.iloc[:,2],mongo_complex.iloc[:,2], postgres_complex.iloc[:,2],
                    fabric_complex.iloc[:,3], fabric_complex_rich.iloc[:,3],mongo_complex.iloc[:,3], postgres_complex.iloc[:,3]],
    keys=['fabric_small_8','rich_small_8','mongo_small_8', 'postgres_small_8',
           'fabric_small_24','rich_small_24','mongo_small_24', 'postgres_small_24',
            'fabric_small_40','rich_small_40','mongo_small_40', 'postgres_small_40',
            'fabric_interm_8','rich_interm_8','mongo_interm_8', 'postgres_interm_8',
            'fabric_interm_24','rich_interm_24','mongo_interm_24', 'postgres_interm_24',
            'fabric_interm_40','rich_interm_40','mongo_interm_40', 'postgres_interm_40',
            'fabric_large_8','rich_large_8','mongo_large_8', 'postgres_large_8',
            'fabric_large_24','rich_large_24','mongo_large_24', 'postgres_large_24',
            'fabric_large_40','rich_large_40','mongo_large_40', 'postgres_large_40',],axis=1, ignore_index=True, copy=False)
    plt.figure(figsize=(13,10), dpi= 80)
    sns.violinplot(data=df, scale='width', inner='quartile', cut=0)
    plt.vlines(2.5, 0, 10000, linestyles ="dotted", colors ="k")
    plt.vlines(5.5, 0, 10000, linestyles ="dotted", colors ="k")
    plt.vlines(8.5, 0, 10000, linestyles ="dotted", colors ="k")
    plt.vlines(11.5, 0, 10000, linestyles ="dotted", colors ="k")
    plt.vlines(14.5, 0, 10000, linestyles ="dotted", colors ="k")
    plt.vlines(17.5, 0, 10000, linestyles ="dotted", colors ="k")
    plt.vlines(20.5, 0, 10000, linestyles ="dotted", colors ="k")
    plt.vlines(23.5, 0, 10000, linestyles ="dotted", colors ="k")
    plt.vlines(26.5, 0, 10000, linestyles ="dotted", colors ="k")
    plt.vlines(29.5, 0, 10000, linestyles ="dotted", colors ="k")
    plt.vlines(32.5, 0, 10000, linestyles ="dotted", colors ="k")
    plt.ylabel("Time [ms]")
    plt.xlabel("K-Assets")
    plt.title('Violin Plot of complex queries', fontsize=22)

# violin plot split into small, medium and large
def plot_beans_complex_sinq():
    large = 22; med = 18; small = 12
    params = {'axes.titlesize': large,
          'legend.fontsize': med,
          'figure.figsize': (16, 10),
          'axes.labelsize': med,
          'axes.titlesize': med,
          'xtick.labelsize': med,
          'ytick.labelsize': med,
          'figure.titlesize': large}
    plt.rcParams.update(params)
    #plt.style.use('seaborn-whitegrid')
    sns.set_style("white")

    fabric_simple = pd.read_excel('../data/violin/query-median-2-8-24-40.ods', sheet_name='FABRIC-SMALL-COMPLEX', 
        header=0, decimal=',')
    fabric_medium = pd.read_excel('../data/violin/query-median-2-8-24-40.ods', sheet_name='FABRIC-INTERMEDIATE-COMPLEX', 
        header=0, decimal=',')
    fabric_complex = pd.read_excel('../data/violin/query-median-2-8-24-40.ods', sheet_name='FABRIC-LARGE-COMPLEX', 
        header=0, decimal=',')
    fabric_simple_rich = pd.read_excel('../data/violin/query-median-2-8-24-40.ods', sheet_name='FABRIC-SMALL-RICH', 
        header=0, decimal=',')
    fabric_medium_rich = pd.read_excel('../data/violin/query-median-2-8-24-40.ods', sheet_name='FABRIC-INTERMEDIATE-RICH', 
        header=0, decimal=',')
    fabric_complex_rich = pd.read_excel('../data/violin/query-median-2-8-24-40.ods', sheet_name='FABRIC-LARGE-RICH', 
        header=0, decimal=',')
    
    df = pd.concat([fabric_simple.iloc[:,0], fabric_simple_rich.iloc[:,0],
                    fabric_simple.iloc[:,2], fabric_simple_rich.iloc[:,2],
                    fabric_simple.iloc[:,3], fabric_simple_rich.iloc[:,3],
                    fabric_medium.iloc[:,0], fabric_medium_rich.iloc[:,0],
                    fabric_medium.iloc[:,2], fabric_medium_rich.iloc[:,2],
                    fabric_medium.iloc[:,3], fabric_medium_rich.iloc[:,3],
                    fabric_complex.iloc[:,0], fabric_complex_rich.iloc[:,0],
                    fabric_complex.iloc[:,2], fabric_complex_rich.iloc[:,2],
                    fabric_complex.iloc[:,3], fabric_complex_rich.iloc[:,3]],
    keys=['fabric_small_2','rich_small_2',
           'fabric_small_24','rich_small_24',
            'fabric_small_40','rich_small_40',
            'fabric_interm_2','rich_interm_2',
            'fabric_interm_24','rich_interm_24',
            'fabric_interm_40','rich_interm_40',
            'fabric_large_2','rich_large_2',
            'fabric_large_24','rich_large_24',
            'fabric_large_40','rich_large40'],axis=1, ignore_index=True, copy=False)
    plt.figure(figsize=(13,10), dpi= 80)
    pal=(['#ec7063','#f5b7b1']*9)
    axis = sns.violinplot(data=df, scale='width', inner='quartile', cut=0,palette=pal)
    max = df.max().max()
    plt.vlines(1.5, 0, max, linestyles ="dotted", colors ="k")
    plt.vlines(3.5, 0, max, linestyles ="dotted", colors ="k")
    plt.vlines(5.5, 0, max, linestyles ="solid", colors ="k")
    plt.vlines(7.5, 0, max, linestyles ="dotted", colors ="k")
    plt.vlines(9.5, 0, max, linestyles ="dotted", colors ="k")
    plt.vlines(11.5, 0, max, linestyles ="solid", colors ="k")
    plt.vlines(13.5, 0, max, linestyles ="dotted", colors ="k")
    plt.vlines(15.5, 0, max, linestyles ="dotted", colors ="k")
    plt.text(1.5,39500,'Small assets',size=18)
    plt.text(7.5,39500,'Medium assets',size=18)
    plt.text(13.5,39500,'Large assets',size=18)
    s=18
    y=-3500
    plt.text(0.5,y,'2',size=s)
    plt.text(2.5,y,'24',size=s)
    plt.text(4.5,y,'40',size=s)
    plt.text(6.5,y,'2',size=s)
    plt.text(8.5,y,'24',size=s)
    plt.text(10.5,y,'40',size=s)
    plt.text(12.5,y,'2',size=s)
    plt.text(14.5,y,'24',size=s)
    plt.text(16.5,y,'40',size=s)
    plt.ylabel("Read time [ms]")
    plt.xticks(np.arange(0,18,step=1),(['']*18))
    plt.xlabel("Stored data [K-assets]",labelpad=20)
    hlf = mpatches.Patch(color='#ec7063', label='HLF (raw)')
    quo = mpatches.Patch(color='#f5b7b1', label='HLF (enhanced)')
    plt.legend(handles=[hlf,quo],loc=5, bbox_to_anchor=(0.3,0.85),shadow=True)
    #plt.title('Violin Plot of complex queries: Fabric and Fabric Rich', fontsize=22)
    plt.savefig('violin_fabric_complex.svg',format='svg')

# violin plot for quorum
def plot_beans_quo():
    large = 22; med = 18; small = 12
    params = {'axes.titlesize': large,
          'legend.fontsize': med,
          'figure.figsize': (16, 10),
          'axes.labelsize': med,
          'axes.titlesize': med,
          'xtick.labelsize': med,
          'ytick.labelsize': med,
          'figure.titlesize': large}
    plt.rcParams.update(params)
    #plt.style.use('seaborn-whitegrid')
    sns.set_style("white")

    quorum_simple = pd.read_excel('../data/violin/query-median-2-8-24-40.ods', sheet_name='QUORUM-SMALL-COMPLEX',
        header=0, decimal=',')
    quorum_medium = pd.read_excel('../data/violin/query-median-2-8-24-40.ods', sheet_name='QUORUM-INTERMEDIATE-COMPLEX',
        header=0, decimal=',')
    quorum_complex = pd.read_excel('../data/violin/query-median-2-8-24-40.ods', sheet_name='QUORUM-LARGE-COMPLEX', 
        header=0, decimal=',')
    
    df = pd.concat([quorum_simple.iloc[:,0],quorum_simple.iloc[:,2],quorum_simple.iloc[:,3],
                    quorum_medium.iloc[:,0],quorum_medium.iloc[:,2],quorum_medium.iloc[:,3],
                    quorum_complex.iloc[:,0],quorum_complex.iloc[:,2],quorum_complex.iloc[:,3]],
     keys=['1','2','3','4','5','6','7','8','9'],axis=1, ignore_index=True, copy=False)
    plt.figure(figsize=(13,10), dpi= 80)
    axis = sns.violinplot(data=df, scale='width', inner='quartile', cut=0)
    max = df.max().max()
    plt.vlines(2.5, 0, max, linestyles ="solid", colors ="k")
    plt.vlines(5.5, 0, max, linestyles ="solid", colors ="k")
    plt.xticks(np.arange(0,9,step=1),(['2','24','40']*3))
    plt.xlabel("K-Assets",labelpad=20)
    plt.ylabel("Time [ms]")
    plt.title('Violin Plot of complex queries: Quorum', fontsize=22)

def annotate_axes(ax, text, fontsize=18):
    ax.text(0.5, 0.5, text, transform=ax.transAxes,
            ha="center", va="center", fontsize=fontsize, color="darkgrey")

# mosaic of violin plots
def plot_beans_mosaico():
    quorum_simple = pd.read_excel('../data/data-quorum/query-median-2-8-24-40.ods', sheet_name='QUORUM-SMALL-COMPLEX',
        header=0, usecols='A,C,D')
    quorum_medium = pd.read_excel('../data/data-quorum/query-median-2-8-24-40.ods', sheet_name='QUORUM-INTERMEDIATE-COMPLEX',
        header=0, usecols='A,B,C')
    quorum_complex = pd.read_excel('../data/data-quorum/query-median-2-8-24-40.ods', sheet_name='QUORUM-LARGE-COMPLEX', 
        header=0, usecols='A,B')
    fig, axd = plt.subplot_mosaic([['upper left','upper','upper right'],
                                ['upper left','upper','upper right'],
                                ['middle left','middle','upper right'],
                                ['middle left','middle','lower right'],
                                ['lower left','lower','lower right'],
                                ['lower left','lower','lower right']],
                                figsize=(10,7),sharex=False, gridspec_kw={'hspace': 0.05, 'wspace': 0.5},)
    s=14

    # small violins
    ax = sns.violinplot(data=quorum_simple, scale='width', inner='quartile', cut=0, ax=axd['lower left'], color='#5499c7')
    ax.set_ylim([2e+3,4e+3])
    ax.set_yticks([2000,2500,3000,3500],['2e3', '2.5e3', '3e3', '3.5e3'])
    ax.set_xticks([0,1,2],[2,24,40])
    ax = sns.violinplot(data=quorum_simple, scale='width', inner='quartile', cut=0, ax=axd['middle left'], color='#5499c7')
    ax.set_ylim([3.2e+05,3.8e+5])
    ax.set_yticks([3.2e+05,3.4e+05,3.6e+05],['3.2e5','3.4e5','3.6e5'])
    ax.set_ylabel('Read time [ms]',fontsize=s)
    ax = sns.violinplot(data=quorum_simple, scale='width', inner='quartile', cut=0, ax=axd['upper left'], color='#5499c7')
    ax.set_ylim([8.5e+05,9.5e+5])
    ax.set_title('Small Asset',fontsize=s)
    ax.set_yticks([8.5e+05,8.75e+05,9e+05,9.25e+05,9.5e+05],['8.5e5','8.75e5','9e5','9.25e5','9.5e5'])

    # medium violins
    ax = sns.violinplot(data=quorum_medium, scale='width', inner='quartile', cut=0, ax=axd['lower'], color='#5499c7')
    ax.set_ylim([2e+4,4e+4])
    ax.set_yticks([2e+04,2.5e+04,3e+04,3.5e+04],['2e4','2.5e4','3e4','3.5e4'])
    ax.set_xticks([0,1,2],[2,8,24])
    ax.set_xlabel('Stored data [k-assets]',fontsize=s)
    ax = sns.violinplot(data=quorum_medium, scale='width', inner='quartile', cut=0, ax=axd['middle'], color='#5499c7')
    ax.set_ylim([5e+05,6e+5])
    ax.set_yticks([5e+05,5.2e+05,5.4e+05,5.6e+05,5.8e+05],['5e5','5.2e5','5.4e5','5.6e5','5.8e5'])
    ax = sns.violinplot(data=quorum_medium, scale='width', inner='quartile', cut=0, ax=axd['upper'], color='#5499c7')
    ax.set_ylim([4e+06,5e+6])
    ax.set_title('Medium Asset',fontsize=s)
    ax.set_yticks([4e+06,4.2e+06,4.4e+06,4.6e+06,4.8e+06,5e6],['4e6','4.2e6','4.4e6','4.6e6','4.8e6','5e6'])
    #ax.yaxis.get_major_formatter().set_scientific(False)
    

    # large violins
    ax = sns.violinplot(data=quorum_complex, scale='width', inner='quartile', cut=0, ax=axd['lower right'], color='#5499c7')
    ax.set_ylim([2.4e+5,3e+5])
    ax.set_yticks([2.4e+5,2.5e+5,2.6e+5,2.7e+5,2.8e+5,2.9e+5],['2.4e5','2.5e5','2.6e5','2.7e5','2.8e5','2.9e5'])
    ax.set_xticks([0,1,],[2,8])
    ax = sns.violinplot(data=quorum_complex, scale='width', inner='quartile', cut=0, ax=axd['upper right'], color='#5499c7')
    ax.set_ylim([3.8e+06,4.3e+6])
    ax.set_title('Large Asset',fontsize=s)
    ax.set_yticks([3.8e+06,3.9e+06,4e+06,4.1e+06,4.2e+06,4.3e+06],['3.8e6','3.9e6','4e6','4.1e6','4.2e6','4.3e6'])
    plt.savefig('violin_quorum_complex.svg',format='svg')
    
    with PdfPages('plot_beans_mosaico.pdf') as pdf:
        pdf.savefig()
        
# key-value violin plot
def plot_beans_kv():
    large = 22; med = 18; small = 12
    params = {'axes.titlesize': large,
          'legend.fontsize': med,
          'figure.figsize': (16, 10),
          'axes.labelsize': med,
          'axes.titlesize': med,
          'xtick.labelsize': med,
          'ytick.labelsize': med,
          'figure.titlesize': large}
    plt.rcParams.update(params)
    #plt.style.use('seaborn-whitegrid')
    sns.set_style("white")

    quorum_small = pd.read_excel('../data/violin/query-median-2-8-24-40.ods', sheet_name='QUORUM-SMALL-KV',
        header=0, decimal=',')
    quorum_interm = pd.read_excel('../data/violin/query-median-2-8-24-40.ods', sheet_name='QUORUM-INTERMEDIATE-KV',
        header=0, decimal=',')
    quorum_large = pd.read_excel('../data/violin/query-median-2-8-24-40.ods', sheet_name='QUORUM-LARGE-KV', 
        header=0, decimal=',')
        
    fabric_small = pd.read_excel('../data/violin/query-median-2-8-24-40.ods', sheet_name='FABRIC-SMALL-KV', 
        header=0, decimal=',')
    fabric_interm = pd.read_excel('../data/violin/query-median-2-8-24-40.ods', sheet_name='FABRIC-INTERMEDIATE-KV', 
        header=0, decimal=',')
    fabric_large = pd.read_excel('../data/violin/query-median-2-8-24-40.ods', sheet_name='FABRIC-LARGE-KV', 
        header=0, decimal=',')
    
    df = pd.concat([fabric_small.iloc[:,0], quorum_small.iloc[:,0],
                    fabric_small.iloc[:,2], quorum_small.iloc[:,2],
                    fabric_small.iloc[:,3], quorum_small.iloc[:,3],
                    fabric_interm.iloc[:,0], quorum_interm.iloc[:,0],
                    fabric_interm.iloc[:,2], quorum_interm.iloc[:,2],
                    fabric_interm.iloc[:,3], quorum_interm.iloc[:,3],
                    fabric_large.iloc[:,0], quorum_large.iloc[:,0],
                    fabric_large.iloc[:,2], quorum_large.iloc[:,2],
                    fabric_large.iloc[:,3], quorum_large.iloc[:,3]],
    keys=['fabric_small_2', 'quorum_small_2',
           'fabric_small_24', 'quorum_small_24',
            'fabric_small_40', 'quorum_small_40',
            'fabric_interm_2', 'quorum_interm_2',
            'fabric_interm_24', 'quorum_interm_24',
            'fabric_interm_40', 'quorum_interm_40',
            'fabric_large_2', 'quorum_large_2'
            'fabric_large_24', 'quorum_large_24'
            'fabric_large_40', 'quorum_large_40','1','2'],axis=1, ignore_index=True, copy=False)
    
    pal=(['#ec7063','#5499c7']*9)
    plt.figure(figsize=(13,10), dpi= 80)
    axis = sns.violinplot(data=df, scale='width', inner='quartile', cut=1,palette=pal)
    #print(df)
    #axis2 = sns.violinplot(data=fabric_medium_rich.iloc[:,3],scale='width', inner='quartile')
    top = df.max().max()
    plt.vlines(1.5, 0, top, linestyles ="dotted", colors ="k")
    plt.vlines(3.5, 0, top, linestyles ="dotted", colors ="k")
    plt.vlines(5.5, 0, top, linestyles ="solid", colors ="k")
    plt.vlines(7.5, 0, top, linestyles ="dotted", colors ="k")
    plt.vlines(9.5, 0, top, linestyles ="dotted", colors ="k")
    plt.vlines(11.5, 0, top, linestyles ="solid", colors ="k")
    plt.vlines(13.5, 0, top, linestyles ="dotted", colors ="k")
    plt.vlines(15.5, 0, top, linestyles ="dotted", colors ="k")
    plt.text(1.5,1150,'Small assets',size=18)
    plt.text(7.5,1150,'Medium assets',size=18)
    plt.text(13.5,1150,'Large assets',size=18)
    s=18
    plt.text(0.5,-100,'2',size=s)
    plt.text(2.5,-100,'24',size=s)
    plt.text(4.5,-100,'40',size=s)
    plt.text(6.5,-100,'2',size=s)
    plt.text(8.5,-100,'24',size=s)
    plt.text(10.5,-100,'40',size=s)
    plt.text(12.5,-100,'2',size=s)
    plt.text(14.5,-100,'24',size=s)
    plt.text(16.5,-100,'40',size=s)
    plt.ylabel("Read time [ms]")
    #plt.xticks(np.arange(0,18,step=1),([8,8,24,24,40,40]*3))
    plt.xticks(np.arange(0,18,step=1),(['']*18))
    plt.xlabel("Stored data [K-assets]",labelpad=20)
    hlf = mpatches.Patch(color='#ec7063', label='HLF')
    quo = mpatches.Patch(color='#5499c7', label='Quorum')
    plt.legend(handles=[hlf,quo],loc=5, bbox_to_anchor=(0.21,0.85),shadow=True)
    #plt.title('Violin Plot of key-value queries', fontsize=22)
    plt.savefig('violin_kv.svg',format='svg')

# lines plot
def complex_new():
    params = {'legend.fontsize': 11,
        'figure.figsize': (16, 10),
        'axes.labelsize': 'x-large',
        'axes.titlesize':'x-large',
        'xtick.labelsize':'14',
        'ytick.labelsize':'14'}
    plt.rcParams.update(params)

    fabric1 = pd.read_excel('../data/data-final/experiments-delta-hlf-mongo.ods', sheet_name='DLT-complex-query', 
        header=0, index_col=0, usecols='A,B,D,F')
        
    fabric2 = pd.read_excel('../data/data-final/experiments-delta-hlf-mongo.ods', sheet_name='DLT-complex-query', 
        header=0, index_col=0, usecols='A,C,E,G')

    quorum = pd.read_excel('../data/data-final/experiments-delta-quorum-mongo.ods', sheet_name='DLT-complex-query', 
        header=0, index_col=0, usecols='A,B,C,D')

    mongo = pd.read_excel('../data/delta-experiments/database.ods', sheet_name='MongoDB-complex-query', 
        header=0, index_col=0)
        
    postgres = pd.read_excel('../data/delta-experiments/database.ods', sheet_name='PostgreSQL-complex-query', 
        header=0, index_col=0)

    plt.plot(quorum.iloc[:,0],color='tab:blue', marker='s', label='Small asset - Quorum')
    plt.plot(fabric1.iloc[:,0],color='tab:red', marker='s', label='Small asset - HLF (raw)')
    plt.plot(fabric2.iloc[:,0],color='tab:red',linestyle='--', marker='s', label='Small asset - HLF (enhanced)')
    plt.plot(mongo.iloc[:,0],color='tab:purple', marker='s', label='Small asset - MongoDB')
    plt.plot(postgres.iloc[:,0],color='tab:green', marker='s', label='Small asset - PostgreSQL')

    plt.plot(quorum.iloc[:,1].dropna(),color='tab:blue', marker='D', label='Medium asset - Quorum')
    plt.plot(fabric1.iloc[:,1].dropna(),color='tab:red', marker='D', label='Medium asset - HLF (raw)')
    plt.plot(fabric2.iloc[:,1].dropna(),color='tab:red',linestyle='--' ,marker='D', label='Medium asset - HLF (enhanced)')
    plt.plot(mongo.iloc[:,1].dropna(),color='tab:purple', marker='D', label='Medium asset - MongoDB')
    plt.plot(postgres.iloc[:,1].dropna(),color='tab:green', marker='D', label='Medium asset - PostgreSQL')

    plt.plot(quorum.iloc[:,2].dropna(),color='tab:blue', marker='o', label='Large asset - Quorum')
    plt.plot(fabric1.iloc[:,2].dropna(),color='tab:red', marker='o', label='Large asset - HLF (raw)')
    plt.plot(fabric2.iloc[:,2].dropna(),color='tab:red',linestyle='--',marker='o', label='Large asset - HLF (enhanced)')
    plt.plot(mongo.iloc[:,2].dropna(),color='tab:purple', marker='o', label='Large asset - MongoDB')
    plt.plot(postgres.iloc[:,2].dropna(),color='tab:green', marker='o', label='Large asset - PostgreSQL')

    plt.yscale("log")
    plt.ylim([0, 1e+08])

    y_labels = [r"$10$", r"$10^{2}$", r"$10^{3}$", r"$10^{4}$", r"$10^{5}$", r"$10^{6}$", r"$10^{7}$"]
    y_ticks = [10, 10**2, 10**3, 10**4, 10**5, 10**6, 10**7]
    plt.yticks(y_ticks, y_labels)

    plt.legend(shadow=False,ncol=3,fontsize=11)
    #plt.title(label='Median of complex queries', fontsize=22)
    plt.ylabel('Read time [ms]', labelpad=12, fontsize=18)
    plt.xlabel('Stored data [K-assets]', labelpad=12,fontsize=18)
    plt.xticks([1000,8000,14000,20000,30000,40000],[1,8,14,20,30,40],fontsize=18)
    #xlabels = np.array(plt.get_xticks()/1000)
    #plt(xlabels.astype(int))
    plt.savefig('complex_lineas.svg', format='svg')

    with PdfPages('complex_new.pdf') as pdf:
        pdf.savefig()

# bar plot
def stacked_hlf():
    proc = pd.read_excel('../data/data-final/delta+mongo_insert_sync_hlf.ods', sheet_name='Hoja1',
        header=0, index_col=0, usecols='A,B,C,D')
    proc = proc.stack().tolist()
    
    ins = pd.read_excel('../data/data-final/delta+mongo_insert_sync_hlf.ods', sheet_name='Hoja1', 
        header=0, index_col=0, usecols='A,E,F,G')
    ins = ins.stack().tolist()

    labels = ['2000','24000','40000','2','24','40','1','3','6']
    fig, ax = plt.subplots()
    ax.bar(labels, ins,label='Insertion')
    ax.bar(labels, proc, bottom=ins, label='Processing')
    plt.legend()

# double bar plot
def stacked_quo():
    large = 20; med = 14; small = 10
    params = {'axes.titlesize': large,
          'legend.fontsize': 12,
          'figure.figsize': (10, 4),
          'axes.labelsize': med,
          'axes.titlesize': med,
          'xtick.labelsize': med,
          'ytick.labelsize': med,
          'figure.titlesize': large
          }
    
    plt.rcParams.update(params)
    plt.rcParams["figure.autolayout"] = True
    
    proc_postgres = pd.read_excel('../data/delta-experiments/delta+db_insert_sync_quorum.ods', sheet_name='POSTGRES',
        header=0, index_col=0, usecols='A,B,C,D')
    proc_postgres = proc_postgres.transpose().stack().tolist()
    
    ins_postgres = pd.read_excel('../data/delta-experiments/delta+db_insert_sync_quorum.ods', sheet_name='POSTGRES', 
        header=0, index_col=0, usecols='A,E,F,G')
    ins_postgres = ins_postgres.transpose().stack().tolist()
    
    proc_mongo = pd.read_excel('../data/delta-experiments/delta+db_insert_sync_quorum.ods', sheet_name='MONGO',
                               header=0, index_col=0, usecols='A,B,C,D')
    proc_mongo = proc_mongo.transpose().stack().tolist()

    ins_mongo = pd.read_excel('../data/delta-experiments/delta+db_insert_sync_quorum.ods', sheet_name='MONGO',
                              header=0, index_col=0, usecols='A,E,F,G')
    ins_mongo = ins_mongo.transpose().stack().tolist()

    labels = ['1','2','3','4','5','6','7','8','9']
    x_pos = [0,1,2,4,5,6,8,9,10]
    fig, ax = plt.subplots()

    bar_width = 0.40

    ax.bar([pos - bar_width / 2 for pos in x_pos], ins_mongo, label='Insertion (MongoDB)', width=bar_width, color='tab:red')
    ax.bar([pos - bar_width / 2 for pos in x_pos], proc_mongo, bottom=ins_mongo, label='Processing (MongoDB)', width=bar_width, color='tab:olive')

    ax.bar([pos + bar_width / 2 for pos in x_pos], ins_postgres, label='Insertion (PostgreSQL)', width=bar_width, color='tab:blue')
    ax.bar([pos + bar_width / 2 for pos in x_pos], proc_postgres, bottom=ins_postgres, label='Processing (PostgreSQL)', width=bar_width, color='tab:orange')

    ax.set_ylim([0,170000])
    plt.legend(loc='center left', fontsize=12)
    index = ['2','24','40']*3
    yfmt = plt.FuncFormatter(numfmt)
    ax.yaxis.set_major_formatter(yfmt)
    plt.xticks(x_pos,index)
    plt.xlabel('Stored data [K-assets]')
    plt.ylabel('Time [s]')
    s=16
    ymin,ymax = ax.get_ylim()
    y = ymax - 15000
    plt.text(0,y,'Small assets',size=s)
    plt.text(4,y,'Medium assets',size=s)
    plt.text(8,y,'Large assets',size=s)
    plt.savefig('stacked2-1.svg', format='svg')

    with PdfPages('stacked_quo_mongo_postgres.pdf') as pdf:
        pdf.savefig()

# bar plot
def stacked_dlt():
    large = 20; med = 14
    params = { 'axes.titlesize': large,
            'legend.fontsize': 12,
            'figure.figsize': (10, 4),
            'axes.labelsize': med,
            'axes.titlesize': med,
            'xtick.labelsize': med,
            'ytick.labelsize': med,
            'figure.titlesize': large }
    plt.rcParams.update(params)
    plt.rcParams['figure.autolayout'] = True

    labels = [2, 24, 40]

    ins_hlf = pd.read_excel('../data/data-final/experiments-delta-hlf-mongo.ods', sheet_name='DLT-insert',
        header=0, index_col=0)
    ins_hlf = ins_hlf.loc[[i * 1000 for i in labels]]
    ins_hlf = ins_hlf.transpose().stack().to_list()

    ins_quo = pd.read_excel('../data/data-final/experiments-delta-quorum-mongo.ods', sheet_name='DLT-insert',
        header=0, index_col=0)
    ins_quo = ins_quo.loc[[i * 1000 for i in labels]]
    ins_quo = ins_quo.transpose().stack().to_list()
    
    _fig, ax = plt.subplots()

    bar_width = 0.4
    x_pos = [i + int(i / 3) for i in range(0, 11)][: 9]
    index = [str(i) for i in labels] * 3

    ax.bar([pos - bar_width / 2 for pos in x_pos], ins_hlf, label='HLF', width=bar_width, color='tab:red')
    ax.bar([pos + bar_width / 2 for pos in x_pos], ins_quo, label='Quorum', width=bar_width, color='tab:blue')

    ymax = 9000000
    ax.set_ylim([0, ymax])
    plt.xticks(x_pos, index)
    plt.xlabel('Stored data [K-assets]')
    plt.ylabel('Time [s]')
    ax.yaxis.set_major_formatter(plt.FuncFormatter(numfmt))
    plt.yticks(range(0, ymax + 1, 2000000))

    s = 16
    y = ymax - 700000
    plt.text(0, y, 'Small assets', size=s)
    plt.text(4, y, 'Medium assets', size=s)
    plt.text(8, y, 'Large assets', size=s)
    plt.legend(loc='center left', fontsize=12)

    plt.savefig('dlt-insertion.svg', format='svg')
    with PdfPages('transaction_dispatch_barplot_hlf_quorum.pdf') as pdf:
        pdf.savefig()

def numfmt(x, _pos):
    return f'{x/1000:,.0f}' 

if __name__ == '__main__':
    stacked_dlt()
    plt.show()
