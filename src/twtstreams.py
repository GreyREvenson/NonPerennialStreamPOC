import os,sys,geopandas,pynhd,twtnamelist

def set_streams(namelist:twtnamelist.Namelist):
    if namelist.options.verbose: print('calling set_streams')
    _set_nhd_flowlines(namelist)

def _set_nhd_flowlines(namelist:twtnamelist.Namelist):
    """Get domain nhd flowlines"""
    if namelist.options.verbose: print('calling _get_nhd_flowlines')
    if not os.path.isfile(namelist.fnames.nhd) or namelist.options.overwrite_flag:
        if not os.path.isfile(namelist.fnames.domain): sys.exit('ERROR could not find '+namelist.fnames.domain)
        domain = geopandas.read_file(namelist.fnames.domain)
        nhd = pynhd.NHDPlusHR("flowline").bygeom(domain.geometry[0])
        nhd.to_file(namelist.fnames.nhd, driver="GPKG")