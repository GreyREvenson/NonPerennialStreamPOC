import os,pygeohydro,twtnamelist,geopandas,sys,shapely,rasterio
    
def set_domain(namelist:twtnamelist.Namelist):
    """Set domain"""
    if namelist.options.verbose: print('calling set_domain')
    _set_domain_boundary(namelist)
    _set_subdomain_paths(namelist)

def _set_domain_boundary(namelist:twtnamelist.Namelist):
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

def _set_subdomain_paths(namelist:twtnamelist.Namelist):
    if namelist.options.verbose: print('calling _set_subdomain_paths')
    domain = geopandas.read_file(namelist.fnames.domain)
    fnames =        [namelist.fnames.domain,
                     namelist.fnames.domain_mask,
                     namelist.fnames.dem,
                     namelist.fnames.dem_breached,
                     namelist.fnames.flow_acc_sca,
                     namelist.fnames.flow_acc_ncells,
                     namelist.fnames.facc_strm_mask,
                     namelist.fnames.slope,
                     namelist.fnames.twi,
                     namelist.fnames.twi_mean,
                     namelist.fnames.soil_texture,
                     namelist.fnames.soil_transmissivity,
                     namelist.fnames.nhd]
    input_dnames =  [namelist.dirnames.wtd_raw,
                     namelist.dirnames.wtd_resampled]
    output_dnames = [namelist.dirnames.output_raw,
                     namelist.dirnames.output_summary]
    if len(domain) == 1:
        for fname in fnames: domain[os.path.basename(fname)] = fname
        for dname in dnames: domain[os.path.basename(fname)] = dname
    else:
        for fname in fnames:
            domain[os.path.basename(fname)] = domain['domain_id'].apply(lambda domain_id: os.path.join(namelist.dirnames.input_subdomain,
                                                                                                       str(domain_id),
                                                                                                       os.path.basename(fname)))
        for fname in fnames:
            for fname_subdomain in domain[os.path.basename(fname)].tolist():
                os.makedirs(os.path.dirname(fname_subdomain), exist_ok=True)
        for dname in input_dnames:
            domain[os.path.basename(dname)] = domain['domain_id'].apply(lambda domain_id: os.path.join(namelist.dirnames.input_subdomain, 
                                                                                                       str(domain_id),
                                                                                                       os.path.basename(dname)))
        for dname in output_dnames:
            domain[os.path.basename(dname)] = domain['domain_id'].apply(lambda domain_id: os.path.join(namelist.dirnames.output_subdomain, 
                                                                                                       str(domain_id),
                                                                                                       os.path.basename(dname)))
        for dname in domain[os.path.basename(dname)].tolist():
            os.makedirs(dname, exist_ok=True)
    if os.path.isfile(namelist.fnames.dem_user):
        domain = domain.to_crs(rasterio.open(namelist.fnames.dem_user,'r').crs.to_string())
    else:
        domain = domain.to_crs('EPSG:4326')
    domain.to_file(namelist.fnames.domain, driver='GPKG')