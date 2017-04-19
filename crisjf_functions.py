
# coding: utf-8

# In[ ]:

#Functions 
import io,urllib2,bz2
from pandas import DataFrame,merge,concat,read_csv

def trade_data(classification='sitc'):
    '''Downloads the world trade data from atlas.media.mit.edu
    Example
    ----------
    >>> world_trade,pnames,cnames = bnt.trade_data('hs96')
    '''
    if classification not in ['sitc','hs92','hs96','hs02','hs07']:
        raise NameError('Invalid classification')
    print 'Retrieving trade data for '+classification
    atlas_url = 'http://atlas.media.mit.edu/static/db/raw/'
    trade_file = {'sitc':'year_origin_sitc_rev2.tsv.bz2',
                  'hs92':'year_origin_hs92_4.tsv.bz2',
                  'hs96':'year_origin_hs96_4.tsv.bz2',
                  'hs02':'year_origin_hs02_4.tsv.bz2',
                  'hs07':'year_origin_hs07_4.tsv.bz2'}
    pname_file = {'sitc':'products_sitc_rev2.tsv.bz2',
                  'hs92':'products_hs_92.tsv.bz2',
                  'hs96':'products_hs_96.tsv.bz2',
                  'hs02':'products_hs_02.tsv.bz2',
                  'hs07':'products_hs_07.tsv.bz2'}

    print 'Downloading country names from '+atlas_url+'country_names.tsv.bz2'
    data = bz2.decompress(urllib2.urlopen(atlas_url+'country_names.tsv.bz2').read())
    cnames = read_csv(io.BytesIO(data),delimiter='\t')[['id_3char','name']].rename(columns={'id_3char':'ccode'}).dropna()
    
    print 'Downloading product names from '+atlas_url + pname_file[classification]
    data = bz2.decompress(urllib2.urlopen(atlas_url + pname_file[classification]).read())
    pnames = read_csv(io.BytesIO(data),delimiter='\t')
    pnames[classification] = pnames[classification].astype(str)
    if classification[:2] == 'hs':
        pnames['id_len'] = pnames[classification].str.len()
        pnames = pnames[pnames['id_len']<=4]
    pnames = pnames[[classification,'name']].rename(columns={classification:'pcode'}).dropna()
    pnames['pcode'] = pnames['pcode'].astype(int)
    pnames = pnames.sort_values(by='pcode')
    
    print 'Downloading trade   data  from '+atlas_url +trade_file[classification]
    data = bz2.decompress(urllib2.urlopen(atlas_url +trade_file[classification]).read())
    world_trade = read_csv(io.BytesIO(data),delimiter='\t')[['year','origin',classification,'export_val','export_rca']].rename(columns={'origin':'ccode',classification:'pcode','export_val':'x'}).dropna()
    world_trade['year'] = world_trade['year'].astype(int)
    world_trade['pcode'] = world_trade['pcode'].astype(int)
    return world_trade,pnames,cnames

