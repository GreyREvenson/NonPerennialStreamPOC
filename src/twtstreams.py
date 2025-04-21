import os,sys,geopandas,pynhd,twtnamelist,whitebox_workflows

def set_streams(namelist:twtnamelist.Namelist):
    if namelist.options.verbose: print('calling set_streams')
    _set_nhd_flowlines(namelist)

def set_stream_mask(namelist:twtnamelist.Namelist):
    """Set mask for flow accumulation delineated stream"""
    if namelist.options.verbose: print('calling set_stream_mask')
    _set_stream_mask(namelist)

def _set_nhd_flowlines(namelist:twtnamelist.Namelist):
    """Get domain nhd flowlines"""
    if namelist.options.verbose: print('calling _set_nhd_flowlines')
    if not os.path.isfile(namelist.fnames.nhd) or namelist.options.overwrite_flag:
        if not os.path.isfile(namelist.fnames.domain): sys.exit('ERROR could not find '+namelist.fnames.domain)
        domain = geopandas.read_file(namelist.fnames.domain)
        nhd = pynhd.NHDPlusHR("flowline").bygeom(domain.geometry[0]).to_crs(domain.crs)
        nhd.to_file(namelist.fnames.nhd, driver="GPKG")

def _set_filldem_fd8facc_stream_mask(namelist:twtnamelist.Namelist):
    """Set mask for flow accumulation delineated stream""" 
    if namelist.options.verbose: print('calling _set_facc_stream_mask')
    if not os.path.isfile(namelist.fnames.facc_strm_mask) or namelist.options.overwrite_flag:
        if not os.path.isfile(namelist.fnames.dem): 
            sys.exit('ERROR _set_facc_stream_mask could not find '+namelist.fnames.dem)
        wbe = whitebox_workflows.WbEnvironment()
        dem_wbe = wbe.read_raster(namelist.fnames.dem)
        dem_filled_wbe = wbe.fill_depressions(dem=dem_wbe)
        facc_fd8_wbe = wbe.fd8_flow_accum(dem=dem_filled_wbe,out_type='sca',log_transform=False)   
        strms = wbe.extract_streams(facc_fd8_wbe, threshold=namelist.vars.facc_strm_threshold, zero_background=True)
        wbe.write_raster(strms, namelist.fnames.facc_strm_mask, compress=False)

def _set_stream_mask(namelist:twtnamelist.Namelist):
    """Set mask for flow accumulation delineated stream""" 
    if namelist.options.verbose: print('calling _set_stream_mask')
    if not os.path.isfile(namelist.fnames.facc_strm_mask) or namelist.options.overwrite_flag:
        if not os.path.isfile(namelist.fnames.flow_acc): 
            sys.exit('ERROR _set_stream_mask could not find '+namelist.fnames.flow_acc)
        wbe = whitebox_workflows.WbEnvironment()
        facc_wbe = wbe.read_raster(namelist.fnames.flow_acc)
        strms = wbe.extract_streams(facc_wbe, threshold=namelist.vars.facc_strm_threshold, zero_background=False)
        wbe.write_raster(strms, namelist.fnames.facc_strm_mask, compress=False)