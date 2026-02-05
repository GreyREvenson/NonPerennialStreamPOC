
import os,numpy,rasterio,shutil,datetime,zipfile,geopandas

def calc_inundation(dt:dict):
    e = _calc_inundation(dt)
    if e is not None: return e
    e = _calc_summary_perc_inundated(dt)
    if e is not None: return e
    e = _calc_strm_permanence(dt)
    if e is not None: return e
    e = _zip_iundation(dt)
    return e

def _zip_iundation(dt):
    domain = dt['domain']
    dirraw = os.path.join(domain.iloc[0]['output'],'raw')
    try:
        if os.path.isdir(dirraw) and len(os.listdir(dirraw)) > 0:
            shutil.make_archive(dirraw, 'zip', dirraw)
            shutil.rmtree(dirraw)
        return None
    except Exception as e:
        return e

def _check_exist(domain:geopandas.GeoDataFrame,dt_start:datetime.datetime,dt_end:datetime.datetime):
    raw    = os.path.join(domain.iloc[0]['output'],'raw')
    rawzip = os.path.join(domain.iloc[0]['output'],'raw.zip')
    inraw, inrawzip = True, True
    idt = dt_start
    while idt <= dt_end:
        dt_str = idt.strftime('%Y%m%d')
        fname  = f'inundation_{dt_str}.tiff'
        if not os.path.isfile(os.path.join(raw,fname)):
            inraw = False
            break
        idt += datetime.timedelta(days=1)
    if os.path.isfile(rawzip):
        with zipfile.ZipFile(rawzip, 'r') as rawarchive:
            zipnl = rawarchive.namelist()
            idt = dt_start
            while idt <= dt_end:
                dt_str = idt.strftime('%Y%m%d')
                fname  = f'inundation_{dt_str}.tiff'
                if fname not in zipnl:
                    inrawzip = False
                    break
                idt += datetime.timedelta(days=1)
    else: inrawzip = False
    if inraw or inrawzip: return False
    else:                 return True
    
def _calc_inundation(dt:dict):
    try:
        domain              = dt['domain']
        dt_start            = dt['dt_start']
        dt_end              = dt['dt_end']
        verbose             = dt['verbose']
        overwrite           = dt['overwrite']
        write_wtd_mean_resampled = dt['write_wtd_mean_resampled'] #write_wtd_mean_flag: if True, write resampled mean WTD grids to domain wtd_resampled directory
        if verbose: print(f'calling _calc_inundation for domain {domain.iloc[0]['domain_id']}',flush=True)
        if overwrite: calc_flag = True
        else:         calc_flag = _check_exist(domain,dt_start,dt_end)
        if calc_flag:
            diroutraw   = os.path.join(domain.iloc[0]['output'],'raw')
            dirinput    = domain.iloc[0]['input']
            twi_local   = rasterio.open(os.path.join(dirinput,'twi.tiff'                ),'r').read(1)
            twi_mean    = rasterio.open(os.path.join(dirinput,'twi_mean.tiff'           ),'r').read(1)
            trans_decay = rasterio.open(os.path.join(dirinput,'soil_transmissivity.tiff'),'r').read(1)
            domain_mask = rasterio.open(os.path.join(dirinput,'domain_mask.tiff'        ),'r').read(1)
            if not os.path.isdir(diroutraw): os.makedirs(diroutraw, exist_ok=True)
            with rasterio.open(os.path.join(dirinput,'dem.tiff'),'r') as riods_dem:
                _dst_crs    = riods_dem.crs
                _dst_shape  = riods_dem.shape
                _dst_meta   = riods_dem.meta.copy()
                _dst_trans  = riods_dem.transform
            idt = dt_start
            while idt <= dt_end:
                dt_str = idt.strftime('%Y%m%d')
                fname_wtd_mean_raw = os.path.join(dirinput,'wtd','raw',f'wtd_{dt_str}.tiff')
                fname_inund        = os.path.join(diroutraw,f'inundation_{dt_str}.tiff')
                if not os.path.isfile(fname_wtd_mean_raw):
                    raise Exception(f'ERROR could not find {fname_wtd_mean_raw}')
                if not os.path.isfile(fname_inund) or overwrite:
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
                        if write_wtd_mean_resampled:
                            resampled_dir = os.path.join(domain.iloc[0]['input'],'wtd','resampled')
                            if not os.path.isdir(resampled_dir): os.makedirs(resampled_dir, exist_ok=True)
                            fname_wtd_mean_warped = os.path.join(resampled_dir,f'wtd_{dt_str}.tiff')
                            if not os.path.isfile(fname_wtd_mean_warped) or overwrite:
                                with rasterio.open(fname_wtd_mean_warped,'w',**_dst_meta) as riods_wtd_mean:
                                    riods_wtd_mean.write(wtd_mean,1)
                idt += datetime.timedelta(days=1)
        return None
    except Exception as e:
        return e

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

def _calc_strm_permanence(dt:dict):
    try:
        domain = dt['domain']
        dt_start  = dt['dt_start']
        dt_end    = dt['dt_end']
        overwrite = dt['overwrite']
        verbose   = dt['verbose']
        if verbose: 
            print(f'calling _calc_strm_permanence for domain {domain.iloc[0]['domain_id']}',flush=True)
        tstr      = f'{dt_start.strftime('%Y%m%d')}_to_{dt_end.strftime('%Y%m%d')}'
        diroutsum = os.path.join(domain.iloc[0]['output'],'summary')
        if not os.path.isdir(diroutsum): os.makedirs(diroutsum, exist_ok=True)
        fname_p  = os.path.join(diroutsum,f'perennial_strms_{tstr}.tiff')
        fname_np = os.path.join(diroutsum,f'nonperennial_strms_{tstr}.tiff')
        fname_in = os.path.join(diroutsum,f'percent_inundated_grid_{tstr}.tiff')
        fname_strm_mask = os.path.join(domain.iloc[0]['input'],'facc_strm_mask.tiff')
        if not os.path.isfile(fname_in):
            raise Exception(f'ERROR could not find percent inundation grid {fname_in}')
        if not os.path.isfile(fname_strm_mask):
            raise Exception(f'ERROR could not find stream mask grid {fname_strm_mask}')
        if not os.path.isfile(fname_p) or not os.path.isfile(fname_np) or overwrite:
            with rasterio.open(fname_in,'r') as riods_piund:
                perc_inund = riods_piund.read(1)
                meta = riods_piund.meta.copy()  
            strms_p = numpy.where(numpy.isclose(perc_inund,100.),1,0).astype(numpy.int8)
            strms   = rasterio.open(fname_strm_mask,'r').read(1).astype(numpy.int8)
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
        return None
    except Exception as e:
        return e

def _calc_summary_perc_inundated(dt:dict):
    domain    = dt['domain']
    dt_start  = dt['dt_start']
    dt_end    = dt['dt_end']
    overwrite = dt['overwrite']
    verbose   = dt['verbose']
    if verbose: 
        print(f'calling _calc_summary_perc_inundated for domain {domain.iloc[0]['domain_id']}',flush=True)
    diroutraw    = os.path.join(domain.iloc[0]['output'],'raw')
    diroutsum    = os.path.join(domain.iloc[0]['output'],'summary')
    if not os.path.isdir(diroutsum): os.makedirs(diroutsum, exist_ok=True)
    fname_output = os.path.join(diroutsum,
                                f'percent_inundated_grid_{dt_start.strftime('%Y%m%d')}_to_{dt_end.strftime('%Y%m%d')}.tiff')
    if not os.path.isfile(fname_output) or overwrite:
        domain_mask = rasterio.open(os.path.join(domain.iloc[0]['input'],'domain_mask.tiff'),'r').read(1)
        sumgrid     = numpy.zeros(shape=domain_mask.shape,dtype=numpy.int32)
        idt         = dt_start
        while idt < dt_end:
            dt_str          = idt.strftime('%Y%m%d')
            fname_inund_dti = os.path.join(diroutraw,
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
