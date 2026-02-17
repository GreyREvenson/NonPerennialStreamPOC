
import os,numpy,rasterio,shutil,datetime,zipfile,geopandas,rioxarray
from rasterio.enums import Resampling

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

def _check_exist(inundation_out_dir:str,dt_start:datetime.datetime,dt_end:datetime.datetime):
    idt = dt_start
    while idt <= dt_end:
        dt_str = idt.strftime('%Y%m%d')
        fname  = f'inundation_{dt_str}.tiff'
        if not os.path.isfile(os.path.join(inundation_out_dir,fname)):
            return True
        idt += datetime.timedelta(days=1)
    return False
    
def calculate_inundation(**kwargs):
    dt_start           = kwargs.get('dt_start',                 None)
    dt_end             = kwargs.get('dt_end',                   None)
    wtd_raw_dir        = kwargs.get('wtd_raw_dir',              None)
    wtd_resampled_dir  = kwargs.get('wtd_resampled_dir',        None)
    inundation_out_dir = kwargs.get('inundation_out_dir',       None)
    fname_twi          = kwargs.get('fname_twi',                None)
    fname_twi_mean     = kwargs.get('fname_twi_mean',           None)
    fname_soil_trans   = kwargs.get('fname_soil_transmissivity',None)
    verbose            = kwargs.get('verbose',                  False)
    overwrite          = kwargs.get('overwrite',                False)
    if verbose: print(f'calling calculate_inundation')
    if overwrite: calc_flag = True
    else:         calc_flag = _check_exist(inundation_out_dir,dt_start,dt_end)
    if calc_flag:
        if verbose: print(f' writing output to {inundation_out_dir}')
        os.makedirs(inundation_out_dir, exist_ok=True)
        twi         = rioxarray.open_rasterio(filename=fname_twi,masked=True).sel(band=1).load()
        twi_mean    = rioxarray.open_rasterio(filename=fname_twi_mean,masked=True).sel(band=1).load()
        soil_transm = rioxarray.open_rasterio(filename=fname_soil_trans,masked=True).sel(band=1).load()
        idt         = dt_start
        while idt <= dt_end:
            dt_str             = idt.strftime('%Y%m%d')
            fname_wtd_mean_raw = os.path.join(wtd_raw_dir,f'wtd_{dt_str}.tiff')
            fname_inund        = os.path.join(inundation_out_dir,f'inundation_{dt_str}.tiff')
            if not os.path.isfile(fname_wtd_mean_raw):
                raise Exception(f'calculate_inundation could not find {fname_wtd_mean_raw}')
            if not os.path.isfile(fname_inund) or overwrite:
                with rioxarray.open_rasterio(fname_wtd_mean_raw,masked=True) as rioxds_wtd_raw, rioxarray.open_rasterio(fname_twi,masked=True) as rioxds_twi:
                    rioxds_wtd_resampled = rioxds_wtd_raw.rio.reproject_match(rioxds_twi, 
                                                                              resampling=Resampling.bilinear)
                    inun_local = _calc_inundation_time_i(wtd_mean    = rioxds_wtd_resampled.sel(band=1),
                                                         twi_local   = twi,
                                                         twi_mean    = twi_mean,
                                                         f           = soil_transm)
                    inun_local.rio.to_raster(fname_inund,compress=True)
                    if wtd_resampled_dir is not None:
                        if not os.path.isdir(wtd_resampled_dir): os.makedirs(wtd_resampled_dir, exist_ok=True)
                        fname_wtd_mean_warped = os.path.join(wtd_resampled_dir,f'wtd_{dt_str}.tiff')
                        rioxds_wtd_resampled.rio.to_raster(fname_wtd_mean_warped,compress=True)
            idt += datetime.timedelta(days=1)
    else:
        if verbose: print(f' found existing inundation calculations in {inundation_out_dir}')

def _calc_inundation_old(dt:dict):
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

def _calc_inundation_time_i(wtd_mean,twi_local,twi_mean,f):
    """Calculate inundation using 
    TOPMODEL-based equation - see Equation 3 of Zhang et al. (2016) (https://doi.org/10.5194/bg-13-1387-2016)
    Arguments:
        wtd_mean:    grid of mean water table depth values  (zeta sub m in equation 3 of Zhang et al.) 
        twi_local:   grid of local TWI values               (lambda sub l in equation 3 of Zhang et al.)
        twi_mean:    grid of mean TWI values                (lamda sub m in equation 3 of Zhang et al.)
        f:           grid of f parameter values             (f in equation 3 and table 1 of Zhang et al.)
    Returns:
        wtd_local:   grid of local water table depth values (zeta sub l in equation 3 of Zhang et al.)
    """
    wtd_mean  = wtd_mean*(-1)                                                             # values must be negative so multiply by -1
    wtd_local = (1/f)*(twi_local-twi_mean)+wtd_mean   # calculate local water depth using equation 3 from Zhang et al. (2016) where domain_mask=1 (i.e., within the model domain), otherise give a NaN value 
    wtd_local = wtd_local.where(wtd_local>=0,numpy.nan)  
    wtd_local = wtd_local.where(wtd_local.isnull(), 1)                          # give value of 1 where local water table depth is >= 0 (i.e. at or above the surface), otherwise give a NaN value
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

def calculate_summary_perc_inundated(**kwargs):
    dt_start           = kwargs.get('dt_start',                 None)
    dt_end             = kwargs.get('dt_end',                   None)
    inundation_raw_dir = kwargs.get('inundation_raw_dir',       None)
    inundation_sum_dir = kwargs.get('inundation_summary_dir',   None)
    fname_dem          = kwargs.get('fname_dem',                None)
    verbose            = kwargs.get('verbose',                  False)
    overwrite          = kwargs.get('overwrite',                False)
    if verbose: print(f'calling calculate_summary_perc_inundated')
    if dt_start is None or not isinstance(dt_start,datetime.datetime) or dt_end is None or not isinstance(dt_end,datetime.datetime):
        raise ValueError(f'calculate_summary_perc_inundated missing required arguments dt_start or dt_end or not valid datetime.datetime objects')
    if inundation_raw_dir is None or not os.path.isdir(inundation_raw_dir):
        raise ValueError(f'calculate_summary_perc_inundated missing required argument inundation_raw_dir or is not valid directory')
    if inundation_sum_dir is None:
        raise ValueError(f'calculate_summary_perc_inundated missing required argument inundation_summary_dir')
    if fname_dem is None or not os.path.isfile(fname_dem):
        raise ValueError(f'calculate_summary_perc_inundated missing required argument fname_dem is not a valid file')
    os.makedirs(inundation_sum_dir, exist_ok=True)
    fname_output = os.path.join(inundation_sum_dir,
                                f'percent_inundated_grid_{dt_start.strftime('%Y%m%d')}_to_{dt_end.strftime('%Y%m%d')}.tiff')
    if not os.path.isfile(fname_output) or overwrite:
        if verbose: print(f' writing summary percent inundation grid {fname_output}')
        sumgrid = rioxarray.open_rasterio(filename=fname_dem,masked=True).sel(band=1).load()
        sumgrid = sumgrid.where(sumgrid.isnull(),0)
        idt     = dt_start
        while idt < dt_end:
            dt_str          = idt.strftime('%Y%m%d')
            fname_inund_dti = os.path.join(inundation_raw_dir,
                                           f'inundation_{dt_str}.tiff')
            inun_dti        = rioxarray.open_rasterio(filename=fname_inund_dti,masked=True).sel(band=1).load()
            sumgrid        += inun_dti
            idt            += datetime.timedelta(days=1)
        perc_inun = (sumgrid/float((dt_end-dt_start).days))*100.
        perc_inun = perc_inun.where(perc_inun>0.,numpy.nan)
        perc_inun.rio.to_raster(fname_output,compress=True)
    else:
        if verbose: print(f' found existing summary percent inundated grid {fname_output}')