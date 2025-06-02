import os,pygeohydro,twtnamelist,geopandas,sys,shapely,rasterio
    
def set_domain(namelist:twtnamelist.Namelist):
    """Set domain"""
    if namelist.options.verbose: print('calling set_domain')
    _set_domain_boundary(namelist)
    _set_hucs(namelist)
    #_set_domain_boundary_buffered(namelist)
    #_set_domain_bbox(namelist)

def _set_domain_boundary(namelist:twtnamelist.Namelist):
    """Set domain spatial boundary"""
    if namelist.options.verbose: print('calling _set_domain_boundary')
    if not os.path.isfile(namelist.fnames.domain) or namelist.options.overwrite_flag:
        hucstr = 'huc'+str(namelist.vars.huc_level)
        wbdbasins = pygeohydro.WBD(hucstr) 
        wbdbasins  = wbdbasins.byids(hucstr,[namelist.vars.huc])
        domain = wbdbasins.dissolve()
        if os.path.isfile(namelist.fnames.dem_user):
            domain = domain.to_crs(rasterio.open(namelist.fnames.dem_user,'r').crs.to_string())
        else:
            domain = domain.to_crs('EPSG:4326')
        domain.to_file(namelist.fnames.domain, driver="GPKG")

def _set_hucs(namelist:twtnamelist.Namelist):
    if namelist.options.verbose: print('calling _set_hucs')
    dtfnames = {'fname_huc'                 : [namelist.dirnames.domain,namelist.fnames.huc],
                'fname_huc_mask'            : [namelist.dirnames.domain,namelist.fnames.huc_mask],
                'fname_dem'                 : [namelist.dirnames.topo,namelist.fnames.dem],
                'fname_dem_breached'        : [namelist.dirnames.topo,namelist.fnames.dem_breached],
                'fname_facc'                : [namelist.dirnames.topo,namelist.fnames.flow_acc],
                'fname_strm_mask'           : [namelist.dirnames.topo,namelist.fnames.facc_strm_mask],
                'fname_slope'               : [namelist.dirnames.topo,namelist.fnames.slope],
                'fname_twi'                 : [namelist.dirnames.topo,namelist.fnames.twi],
                'fname_twi_mean'            : [namelist.dirnames.topo,namelist.fnames.twi_mean],
                'fname_soil_texture'        : [namelist.dirnames.soils,namelist.fnames.soil_texture],
                'fname_soil_transmissivity' : [namelist.dirnames.soils,namelist.fnames.soil_transmissivity],
                'fname_nhd'                 : [namelist.dirnames.nhd,namelist.fnames.nhd]}
    dtdnames = {'dirname_wtd_raw'           : namelist.dirnames.wtd_parflow_raw,
                'dirname_wtd_reprj_resmple' : namelist.dirnames.wtd_parflow_resampled,
                'dirname_wtd_output_raw'    : namelist.dirnames.output_raw,
                'dirname_wtd_output_summary': namelist.dirnames.output_summary}
    if not os.path.isfile(namelist.fnames.hucs) or namelist.options.overwrite_flag:
        domain  = geopandas.read_file(namelist.fnames.domain)
        #domain_geom = shapely.ops.unary_union(domain.to_crs('EPSG:4326').geometry)
        brk_lvl = namelist.pp.huc_break_lvl
        if not isinstance(brk_lvl,int): 
            brk_lvl = len(str(namelist.vars.huc))
        if brk_lvl not in (2,4,6,8,10,12): 
            sys.exit(f'ERROR _set_hucs invalid huc level {brk_lvl}')
        hucid_colname = f'huc{brk_lvl}'
        hucs = pygeohydro.watershed.huc_wb_full(brk_lvl)
        #hucs          = pygeohydro.WBD(layer=f'huc{brk_lvl}',crs='EPSG:4326') #EPSG:4326 is default
        #hucs          = hucs.bygeom(geom=domain_geom,geo_crs=domain.crs,huc_level)
        hucs          = hucs[hucs[hucid_colname].str.startswith(namelist.vars.huc)]
        hucs          = hucs.drop(columns=[col for col in hucs.columns if col not in [hucid_colname,'geometry']]) 
        for colname in dtfnames:
            hucs[colname] = hucs[hucid_colname].apply(lambda hucid:
                                                        os.path.join(dtfnames[colname][0], f'huc_{hucid}', dtfnames[colname][1]))
        for colname in dtdnames:
            hucs[colname] = hucs[hucid_colname].apply(lambda hucid:
                                                        os.path.join(dtdnames[colname], f'huc_{hucid}'))
        #hucs['fname_domain']               = hucs[colname].apply(lambda hucid: 
        #                                     os.path.join(namelist.dirnames.domain, f'huc_{hucid}', namelist.fnames.domain))
        hucs = hucs.set_index(hucid_colname,verify_integrity=True)
        hucs.to_file(namelist.fnames.hucs,index=True)
    else:
        hucs = geopandas.read_file(namelist.fnames.hucs)
    for colname in dtfnames:
        for fname in hucs[colname].tolist():
            if not os.path.isdir(os.path.dirname(fname)):
                os.mkdir(os.path.dirname(fname))
    for colname in dtdnames:
        for dirname in hucs[colname].tolist():
            if not os.path.isdir(dirname):
                os.mkdir(dirname)