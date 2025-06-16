import os,pygeohydro,twtnamelist,geopandas,sys,shapely,rasterio
    
def set_domain(namelist:twtnamelist.Namelist):
    """Set domain"""
    if namelist.options.verbose: print('calling set_domain')
    _set_domain(namelist)
    _set_domain_fnames(namelist)

def _set_domain(namelist:twtnamelist.Namelist):
    """Set domain - TODO: change so that domain can be a non-huc area and subdomain units can be non-huc area"""
    if namelist.options.verbose: print('calling _set_domain_boundary')
    if not os.path.isfile(namelist.fnames.domain) or namelist.options.overwrite_flag:
        name_domain = namelist.options.name_domain
        if isinstance(name_domain, str) and len(name_domain) in (2,4,6,8,10,12,14,16):
            # domain is a huc
            if isinstance(namelist.options.huc_break_lvl, int):
                # domain = multi row gdf
                colnam = f'huc{namelist.options.huc_break_lvl}'
                hucs   = pygeohydro.watershed.huc_wb_full(namelist.options.huc_break_lvl)
                domain = hucs[hucs[colnam].str.startswith(name_domain)]
                domain = domain.drop(columns=[col for col in domain.columns if col not in [colnam,'geometry']]) 
                domain = domain.rename(columns={f'huc{namelist.options.huc_break_lvl}' : 'domain_id'})
            else:
                # domain = single row gdf
                colnam = 'huc' + str(len(name_domain))
                basins = pygeohydro.WBD(colnam) 
                domain = basins.byids(colnam, [name_domain])
                domain = domain.dissolve(by = colnam)
                domain = domain.rename(columns = {colnam : 'domain_id'})
        else:
            # domain is non-huc shape
            sys.exit('ERROR: currently, domain must be a HUC')
        domain.to_file(namelist.fnames.domain, driver='GPKG')

def _set_domain_fnames(namelist:twtnamelist.Namelist):
    if namelist.options.verbose: print('calling _set_domain_fnames')
    domain   = geopandas.read_file(namelist.fnames.domain)
    dtfnames = {'fname_domain'              : namelist.fnames.domain,
                'fname_domain_mask'         : namelist.fnames.domain_mask,
                'fname_dem'                 : namelist.fnames.dem,
                'fname_dem_breached'        : namelist.fnames.dem_breached,
                'fname_facc'                : namelist.fnames.flow_acc,
                'fname_strm_mask'           : namelist.fnames.facc_strm_mask,
                'fname_slope'               : namelist.fnames.slope,
                'fname_twi'                 : namelist.fnames.twi,
                'fname_twi_mean'            : namelist.fnames.twi_mean,
                'fname_soil_texture'        : namelist.fnames.soil_texture,
                'fname_soil_transmissivity' : namelist.fnames.soil_transmissivity,
                'fname_nhd'                 : namelist.fnames.nhd}
    dtdnames = {'dirname_wtd_raw'           : namelist.dirnames.wtd_parflow_raw,
                'dirname_wtd_reprj_resmple' : namelist.dirnames.wtd_parflow_resampled,
                'dirname_wtd_output_raw'    : namelist.dirnames.output_raw,
                'dirname_wtd_output_summary': namelist.dirnames.output_summary}
    if len(domain) == 1:
        for fname in dtfnames:   domain[fname]   = dtfnames[fname]
        for dirname in dtdnames: domain[dirname] = dtdnames[dirname]
    else:
        for fname in dtfnames:
            if fname not in domain.columns:
                domain[fname] = domain['domain_id'].apply(lambda domain_id: os.path.join(os.path.dirname(dtfnames[fname]), 
                                                                                         '_subdomains',
                                                                                         str(domain_id),
                                                                                         os.path.basename(dtfnames[fname])))
            for _fname in domain[fname].tolist():
                os.makedirs(os.path.dirname(_fname), exist_ok=True)
        for dname in dtdnames:
            if dname not in domain.columns:
                domain[dname] = domain['domain_id'].apply(lambda domain_id: os.path.join(dtdnames[dname], 
                                                                                        '_subdomains',
                                                                                        str(domain_id)))
            for _dname in domain[dname].tolist():
                os.makedirs(_dname, exist_ok=True)
    if os.path.isfile(namelist.fnames.dem_user):
        domain = domain.to_crs(rasterio.open(namelist.fnames.dem_user,'r').crs.to_string())
    else:
        domain = domain.to_crs('EPSG:4326')
    domain.to_file(namelist.fnames.domain, driver='GPKG')