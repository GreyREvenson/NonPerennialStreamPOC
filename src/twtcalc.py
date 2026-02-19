
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
    if dt_start is None or not isinstance(dt_start,datetime.datetime):
        raise ValueError(f'calculate_inundation missing required argument dt_start or is not a valid datetime')
    if dt_end is None or not isinstance(dt_end,datetime.datetime):
        raise ValueError(f'calculate_inundation missing required argument dt_end or is not a valid datetime')

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
                    inun_local.rio.to_raster(fname_inund,compress='LZMA')
                    if wtd_resampled_dir is not None:
                        if not os.path.isdir(wtd_resampled_dir): os.makedirs(wtd_resampled_dir, exist_ok=True)
                        fname_wtd_mean_warped = os.path.join(wtd_resampled_dir,f'wtd_{dt_str}.tiff')
                        rioxds_wtd_resampled.rio.to_raster(fname_wtd_mean_warped,compress='LZMA')
            idt += datetime.timedelta(days=1)
    else:
        if verbose: print(f' found existing inundation calculations in {inundation_out_dir}')

def _calc_inundation_time_i(wtd_mean,twi_local,twi_mean,f):
    """Calculate inundation using 
    TOPMODEL-based equation - see Equation 3 of Zhang et al. (2016) (https://doi.org/10.5194/bg-13-1387-2016)
    Arguments:
        wtd_mean:    grid of mean water table depth values  (zeta sub m in equation 3 of Zhang et al.) 
        twi_local:   grid of local TWI values               (lambda sub l in equation 3 of Zhang et al.)
        twi_mean:    grid of mean TWI values                (lamda sub m in equation 3 of Zhang et al.)
        f:           grid of f parameter values             (f in equation 3 and table 1 of Zhang et al.)
    Intermediate calculation:
        wtd_local:   grid of local water table depth values (zeta sub l in equation 3 of Zhang et al.)
    Returns:
        inun_local:  grid of surface inundation (1=yes,0=no/nondata)
    """
    wtd_mean  = wtd_mean*(-1)                                                  # values must be negative so multiply by -1
    wtd_local = (1/f)*(twi_local-twi_mean)+wtd_mean                            # calculate local water depth using equation 3 from Zhang et al. (2016) where domain_mask=1 (i.e., within the model domain), otherise give a NaN value 
    wtd_local = wtd_local.where(wtd_local>=0,numpy.nan)  
    inun_local = wtd_local.where(wtd_local.isnull(), 1)                        # give value of 1 where local water table depth is >= 0 (i.e. at or above the surface), otherwise give a NaN value
    inun_local = inun_local.where(~inun_local.isnull(),0).astype(numpy.uint8)
    inun_local.rio.write_nodata(0, inplace=True)
    return inun_local

def calculate_strm_permanence(**kwargs):
    fname_perc_inundation    = kwargs.get('fname_perc_inundation',    None)
    fname_dem                = kwargs.get('fname_dem',                None)
    fname_strm_mask          = kwargs.get('fname_strm_mask',          None)
    verbose                  = kwargs.get('verbose',                  False)
    overwrite                = kwargs.get('overwrite',                False)
    if verbose: print(f'calling calculate_strm_permanence')
    if fname_perc_inundation is None:
        raise ValueError(f'calculate_strm_permanence missing required argument fname_perc_inundation')
    if not os.path.isfile(fname_perc_inundation):
        raise ValueError(f'calculate_strm_permanence argument fname_perc_inundation {fname_perc_inundation} is not valid file')
    if fname_dem is None:
        raise ValueError(f'calculate_strm_permanence missing required argument fname_dem')
    if not os.path.isfile(fname_dem):
        raise ValueError(f'calculate_strm_permanence argument fname_dem {fname_dem} is not valid file')
    if fname_strm_mask is None:
        raise ValueError(f'calculate_strm_permanence missing required argument fname_strm_mask')
    if not os.path.isfile(fname_strm_mask):
        raise ValueError(f'calculate_strm_permanence argument fname_strm_mask {fname_strm_mask} is not valid file')
    try:
        dstr = str(os.path.basename(fname_perc_inundation))
        dstr = dstr.replace('.tiff','').replace('percent_inundated_grid_','')
    except:
        raise ValueError(f'calculate_strm_permanence could not parse percent inundation file name {fname_perc_inundation}')
    fname_p  = os.path.join(os.path.dirname(fname_perc_inundation),
                            f'perennial_strms_{dstr}.tiff')
    fname_np = os.path.join(os.path.dirname(fname_perc_inundation),
                            f'nonperennial_strms_{dstr}.tiff')
    if not os.path.isfile(fname_p) or not os.path.isfile(fname_np) or overwrite:
        if verbose: print(f' writing {fname_p}')
        atol = 1.e-6 #for float comparisons
        perc_inundation  = rioxarray.open_rasterio(fname_perc_inundation,masked=True).sel(band=1).load()
        strm_mask        = rioxarray.open_rasterio(fname_strm_mask,masked=True).sel(band=1).load()
        strm_mask        = strm_mask.where(numpy.isclose(strm_mask,1.,atol=atol),numpy.nan)
        perc_inund_strms = perc_inundation.where(~strm_mask.isnull(),numpy.nan)
        strms_p          = perc_inund_strms.where(numpy.isclose(perc_inund_strms,100.,atol=atol),numpy.nan)
        strms_p.rio.to_raster(fname_p,compress='LZMA')
        if verbose: print(f' writing {fname_np}')
        strms_np         = perc_inund_strms.where(perc_inund_strms<(100.-atol),numpy.nan)
        strms_np         = strms_np.where(strms_np>0.,numpy.nan)
        strms_np.rio.to_raster(fname_np,compress='LZMA')

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
        perc_inun.rio.to_raster(fname_output,compress='LZMA')
    else:
        if verbose: print(f' found existing summary percent inundated grid {fname_output}')
    return fname_output