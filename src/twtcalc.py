
import os,numpy,rasterio,twtnamelist,multiprocessing,geopandas,datetime,twtutils

def calc_parflow_inundation(namelist:twtnamelist.Namelist):
    """Calculate inundation using ParFlow simulations"""
    if namelist.options.verbose: print('calling calc_parflow_inundation')
    _calc_inundation_main(namelist)
    _calc_parflow_inundation_summary(namelist)

def _calc_parflow_inundation_summary(namelist:twtnamelist.Namelist):
    """Calculate summary grids"""
    if namelist.options.verbose: print('calling _calc_parflow_inundation_summary')
    _calc_summary_perc_inundated_main(namelist)
    _calc_strm_permanence_main(namelist)

def _calc_parflow_inundation_time_i(wtd_mean,twi_local,twi_mean,f,domain_mask):
    """Calculate inundation using 
    TOPMODEL-based equation - see Equation 3 of Zhang et al. (2016) (https://doi.org/10.5194/bg-13-1387-2016)
    Arguments:
        wtd_mean:    grid of mean water table depth values  (zeta sub m in equation 3 of Zhang et al.) 
        twi_local:   grid of local TWI values               (lambda sub l in equation 3 of Zhang et al.)
        twi_mean:    grid of mean TWI values                (lamda sub m in equation 3 of Zhang et al.)
        f:           grid of f parameter values             (f in equation 3 and table 1 of Zhang et al.)
        domain_mask: grid of model domain                   (1=domain,0=not domain)
    Returns:
        wtd_local:   grid of local water table depth values (zeta sub l in equation 3 of Zhang et al.)
    """
    wtd_mean  = wtd_mean*(-1)                                                             # values must be negative so multiply by -1
    wtd_local = numpy.where(domain_mask==1,(1/f)*(twi_local-twi_mean)+wtd_mean,numpy.nan) # calculate local water depth using equation 3 from Zhang et al. (2016) where domain_mask=1 (i.e., within the model domain), otherise give a NaN value 
    wtd_local = numpy.where(wtd_local>=0,1,numpy.nan)                                     # give value of 1 where local water table depth is >= 0 (i.e. at or above the surface), otherwise give a NaN value
    return wtd_local

def _calc_strm_permanence_main(namelist:twtnamelist.Namelist):
    if namelist.options.verbose: print('calling _calc_strm_perenniality_main')
    domain = geopandas.read_file(namelist.fnames.domain)
    args = list()
    for i,r in domain.iterrows():
        tstr = f'{dt_start.strftime('%Y%m%d')}_to_{dt_end.strftime('%Y%m%d')}'
        fname_p = os.path.join(r['dirname_wtd_output_summary'],f'perennial_strms_{tstr}.tiff')
        fname_np = os.path.join(r['dirname_wtd_output_summary'],f'nonperennial_strms_{tstr}.tiff')
        if not os.path.isfile(fname_p) or not os.path.isfile(fname_np) or namelist.options.overwrite_flag:
            args.append([domain.loc[[i]],namelist.time.datetime_dim[0],namelist.time.datetime_dim[-1]])
    if namelist.options.pp:
        twtutils.pp_func(_calc_strm_permanence,args,namelist)
    else:
        for arg in args: _calc_strm_permanence(*arg)
    for i,r in domain.iterrows():
        tstr = f'{dt_start.strftime('%Y%m%d')}_to_{dt_end.strftime('%Y%m%d')}'
        fname_p = os.path.join(r['dirname_wtd_output_summary'],f'perennial_strms_{tstr}.tiff')
        if not os.path.isfile(fname_p):
            print(f'ERROR _calc_strm_permanence_main missing {os.path.basename(fname_p)} for domain {r['domain_id']}')
        fname_np = os.path.join(r['dirname_wtd_output_summary'],f'nonperennial_strms_{tstr}.tiff')
        if not os.path.isfile(fname_np):
            print(f'ERROR _calc_strm_permanence_main missing {os.path.basename(fname_p)} for domain {r['domain_id']}')

def _calc_strm_permanence(domain:geopandas.GeoDataFrame,dt_start:datetime.datetime,dt_end:datetime.datetime):
    tstr = f'{dt_start.strftime('%Y%m%d')}_to_{dt_end.strftime('%Y%m%d')}'
    fname_p  = os.path.join(domain.iloc[0]['dirname_wtd_output_summary'],f'perennial_strms_{tstr}.tiff')
    fname_np = os.path.join(domain.iloc[0]['dirname_wtd_output_summary'],f'nonperennial_strms_{tstr}.tiff')
    if not os.path.isfile(fname_p) or not os.path.isfile(fname_np) or overwrite_flag:
        strms    = rasterio.open(domain.iloc[0]['fname_strm_mask'],'r').read(1).astype(numpy.int8)
        fname_in = os.path.join(domain.iloc[0]['dirname_wtd_output_summary'],f'percent_inundated_grid_{tstr}.tiff')
        with rasterio.open(fname_in,'r') as riods_piund:
            perc_inund = riods_piund.read(1)
            meta = riods_piund.meta.copy()  
        strms_p = numpy.where(numpy.isclose(perc_inund,100.),1,0).astype(numpy.int8)
        strms_p = numpy.where(strms==1,strms_p,0)
        meta.update({'dtype':numpy.int8,
                     'nodata':0})
        with rasterio.open(fname_p, "w", **meta) as riods_out:
            riods_out.write(strms_p,1)
        strms_np = numpy.where((perc_inund>0)&(perc_inund<100),perc_inund,numpy.nan)
        strms_np = numpy.where(strms==1,strms_np,numpy.nan)
        meta.update({'dtype':numpy.float32,
                     'nodata':numpy.nan})
        with rasterio.open(fname_np, "w", **meta) as riods_out:
            riods_out.write(strms_np,1)

def _calc_summary_perc_inundated_main(namelist:twtnamelist.Namelist):
    """Calculate summary grid of inundated area"""
    if namelist.options.verbose: print('calling _calc_summary_perc_inundated_main')
    domain = geopandas.read_file(namelist.fnames.domain)
    args = list()
    for i,r in domain.iterrows():
        if not os.path.isfile(os.path.join(r['dirname_wtd_output_summary'],
                              f'percent_inundated_grid_{namelist.time.datetime_dim[0].strftime('%Y%m%d')}_to_{namelist.time.datetime_dim[-1].strftime('%Y%m%d')}.tiff')):
            args.append([domain.loc[[i]],namelist.time.datetime_dim[0],namelist.time.datetime_dim[-1]])
    if namelist.options.pp:
        twtutils.pp_func(_calc_summary_perc_inundated,args,namelist)
    else:
        for arg in args: _calc_summary_perc_inundated(*arg)
    for i,r in domain.iterrows():
        tstr = f'{namelist.time.datetime_dim[0].strftime('%Y%m%d')}_to_{namelist.time.datetime_dim[-1].strftime('%Y%m%d')}'
        if not os.path.isfile(os.path.join(r['dirname_wtd_output_summary'],f'percent_inundated_grid_{tstr}.tiff')):
            print(f'ERROR _calc_summary_perc_inundated_main missing summary percent inundation calculations for domain {r['domain_id']}')

def _calc_summary_perc_inundated(domain:geopandas.GeoDataFrame,dt_start:datetime.datetime,dt_end:datetime.datetime,overwrite_flag:bool=False):
    domain_mask = rasterio.open(domain.iloc[0]['fname_domain_mask'],'r').read(1)
    fname_output = os.path.join(domain.iloc[0]['dirname_wtd_output_summary'],
                                f'percent_inundated_grid_{dt_start.strftime('%Y%m%d')}_to_{dt_end.strftime('%Y%m%d')}.tiff')
    if not os.path.isfile(fname_output) or overwrite_flag:
        sumgrid = numpy.zeros(shape=domain_mask.shape,dtype=numpy.int32)
        idt     = dt_start
        while idt < dt_end:
            dt_str          = idt.strftime('%Y%m%d')
            fname_inund_dti = os.path.join(domain.iloc[0]['dirname_wtd_output_raw'],
                                           f'inundation_{dt_str}.tiff')
            inun_dti        = rasterio.open(fname_inund_dti,'r').read(1)
            sumgrid        += inun_dti
            idt            += datetime.timedelta(days=1)
        perc_inun = (sumgrid.astype(numpy.float64)/float((dt_end-dt_start).days))*100.
        perc_inun = numpy.where(perc_inun==0.,numpy.nan,perc_inun)
        perc_inun = perc_inun.astype(numpy.float32)
        perc_inun_meta = rasterio.open(fname_inund_dti,'r').meta.copy()
        perc_inun_meta.update({'dtype'  : numpy.float32,
                               'nodata' : numpy.nan})
        with rasterio.open(fname_output, "w", **perc_inun_meta) as riods_perc_inund:
            riods_perc_inund.write(perc_inun,1)

def _calc_inundation_time_i(wtd_mean,twi_local,twi_mean,f,domain_mask):
    """Calculate inundation using 
    TOPMODEL-based equation - see Equation 3 of Zhang et al. (2016) (https://doi.org/10.5194/bg-13-1387-2016)
    Arguments:
        wtd_mean:    grid of mean water table depth values  (zeta sub m in equation 3 of Zhang et al.) 
        twi_local:   grid of local TWI values               (lambda sub l in equation 3 of Zhang et al.)
        twi_mean:    grid of mean TWI values                (lamda sub m in equation 3 of Zhang et al.)
        f:           grid of f parameter values             (f in equation 3 and table 1 of Zhang et al.)
        domain_mask: grid of model domain                   (1=domain,0=not domain)
    Returns:
        wtd_local:   grid of local water table depth values (zeta sub l in equation 3 of Zhang et al.)
    """
    wtd_mean  = wtd_mean*(-1)                                                             # values must be negative so multiply by -1
    wtd_local = numpy.where(domain_mask==1,(1/f)*(twi_local-twi_mean)+wtd_mean,numpy.nan) # calculate local water depth using equation 3 from Zhang et al. (2016) where domain_mask=1 (i.e., within the model domain), otherise give a NaN value 
    wtd_local = numpy.where(wtd_local>=0,1,numpy.nan)                                     # give value of 1 where local water table depth is >= 0 (i.e. at or above the surface), otherwise give a NaN value
    return wtd_local

def _calc_inundation(domain:geopandas.GeoDataFrame,dt_start:datetime.datetime,dt_end:datetime.datetime,write_wtd_mean_flag=False):
    try:
        twi_local   = rasterio.open(domain.iloc[0]['fname_twi'],'r').read(1)
        twi_mean    = rasterio.open(domain.iloc[0]['fname_twi_mean'],'r').read(1)
        trans_decay = rasterio.open(domain.iloc[0]['fname_soil_transmissivity'],'r').read(1)
        domain_mask = rasterio.open(domain.iloc[0]['fname_domain_mask'],'r').read(1)
        with rasterio.open(domain.iloc[0]['fname_dem'],'r') as riods_dem:
            _dst_crs    = riods_dem.crs
            _dst_shape  = riods_dem.shape
            _dst_meta   = riods_dem.meta.copy()
            _dst_trans  = riods_dem.transform
        idt = dt_start
        while idt <= dt_end:
            dt_str = idt.strftime('%Y%m%d')
            fname_wtd_mean_raw = os.path.join(domain.iloc[0]['dirname_wtd_raw'],f'wtd_{dt_str}.tiff')
            fname_inund        = os.path.join(domain.iloc[0]['dirname_wtd_output_raw'],f'inundation_{dt_str}.tiff')
            if not os.path.isfile(fname_inund):
                with rasterio.open(fname_wtd_mean_raw,'r') as riods_wtd_mean_raw:
                    wtd_mean, wtd_mean_trans = rasterio.warp.reproject(source        = riods_wtd_mean_raw.read(1),
                                                                       destination   = numpy.empty(shape=_dst_shape,dtype=numpy.float32),
                                                                       src_transform = riods_wtd_mean_raw.transform,
                                                                       src_crs       = riods_wtd_mean_raw.crs,
                                                                       dst_transform = _dst_trans,
                                                                       dst_crs       = _dst_crs,
                                                                       resampling    = rasterio.enums.Resampling.bilinear)
                    inun_local = _calc_inundation_time_i(wtd_mean    = wtd_mean,
                                                         twi_local   = twi_local,
                                                         twi_mean    = twi_mean,
                                                         f           = trans_decay,
                                                         domain_mask = domain_mask)
                    inun_local = numpy.where(inun_local==1,1,0).astype(numpy.int8)
                    inun_local_meta = _dst_meta.copy()
                    inun_local_meta.update({'transform' : wtd_mean_trans,
                                            'dtype'     : rasterio.int8,
                                            'nodata'    : 0})
                    with rasterio.open(fname_inund,'w',**inun_local_meta) as riods_wtd_local:
                        riods_wtd_local.write(inun_local,1)
                    if write_wtd_mean_flag:
                        fname_wtd_mean_warped = os.path.join(domain.iloc[0]['dirname_wtd_reprj_resmple'],f'wtd_{dt_str}.tiff')
                        if not os.path.isfile(fname_wtd_mean_warped) or overwrite_flag:
                            with rasterio.open(fname_wtd_mean_warped,'w',**_dst_meta) as riods_wtd_mean:
                                riods_wtd_mean.write(wtd_mean,1)
            idt += datetime.timedelta(days=1)
        return [None, None]
    except Exception as e:
        return [domain.iloc[0]['domain_id'], e]

def _calc_inundation_main(namelist:twtnamelist.Namelist):
    if namelist.options.verbose: print('calling _calc_inundation_main')
    domain = geopandas.read_file(namelist.fnames.domain)
    args = list()
    for i,r in domain.iterrows():
        f = False
        for dt in namelist.time.datetime_dim:
            if not os.path.isfile(os.path.join(r['dirname_wtd_output_raw'],f'inundation_{dt.strftime('%Y%m%d')}')):
                f = True
                break
        if f:
            args.append([domain.loc[[i]],namelist.time.datetime_dim[0],namelist.time.datetime_dim[-1],False])
    if namelist.options.pp:
        twtutils.pp_func(_calc_inundation,args,namelist)
    else:
        for arg in args: _calc_inundation(*arg)
    for i,r in domain.iterrows():
        if not all([os.path.isfile(os.path.join(r['dirname_wtd_output_raw'],f'inundation_{dt.strftime('%Y%m%d')}')) 
                    for dt in namelist.time.datetime_dim]):
            print(f'ERROR _calc_inundation_main missing inundation calculations for domain {r['domain_id']}')