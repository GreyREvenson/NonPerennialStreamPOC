
import os,numpy,shutil,datetime,rioxarray,xarray,sys
from rasterio.enums import Resampling

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
    wtd_mean   = numpy.where(~numpy.isnan(wtd_mean),wtd_mean*(-1),numpy.nan)                                                  # values must be negative so multiply by -1
    wtd_local  = numpy.where(~numpy.isnan(wtd_mean),(1/f)*(twi_local-twi_mean)+wtd_mean,numpy.nan) 
    inun_local = numpy.where(wtd_local>=0,1,numpy.nan)
    return inun_local

def calculate_strm_permanence(**kwargs):
    fname_perc_inundation    = kwargs.get('fname_perc_inundation',    None)
    fname_dem                = kwargs.get('fname_dem',                None)
    fname_strm_mask          = kwargs.get('fname_strm_mask',          None)
    verbose                  = kwargs.get('verbose',                  False)
    overwrite                = kwargs.get('overwrite',                False)
    fname_verbose            = kwargs.get('fname_verbose',            None)
    chunks                   = kwargs.get('chunks',                   None)
    if fname_verbose is not None:
        f = open(fname_verbose, "a", buffering=1)
        sys.stdout = f
        sys.stderr = f
    if verbose: print(f'calling calculate_strm_permanence')
    dstr = str(os.path.basename(fname_perc_inundation)).replace('.tiff','').replace('percent_inundated_grid_','')
    fname_p  = os.path.join(os.path.dirname(fname_perc_inundation),f'perennial_strms_{dstr}.tiff')
    fname_np = os.path.join(os.path.dirname(fname_perc_inundation),f'nonperennial_strms_{dstr}.tiff')
    if not os.path.isfile(fname_p) or not os.path.isfile(fname_np) or overwrite:
        if verbose: print(f' writing {fname_p}')
        atol = 1.e-6 #for float comparisons
        if chunks is not None:
            perc_inundation  = rioxarray.open_rasterio(filename=fname_perc_inundation,masked=True,chunks=chunks)
            strm_mask        = rioxarray.open_rasterio(filename=fname_strm_mask,masked=True,chunks=chunks)
            perc_inund_strms = numpy.where(strm_mask==1,perc_inundation,numpy.nan)
            strms_p          = numpy.where(numpy.isclose(perc_inund_strms,100.,atol=atol),1,numpy.nan)
            strms_p.rio.to_raster(fname_p)
            if verbose: print(f' writing {fname_np}')
            strms_np         = numpy.where(strm_mask==1&
                                        numpy.where(perc_inund_strms<(100.-atol)&
                                        numpy.where(perc_inund_strms>0.)),
                                        perc_inund_strms,
                                        numpy.nan)
            strms_np.rio.to_raster(fname_np)
        else:
            perc_inundation  = rioxarray.open_rasterio(filename=fname_perc_inundation,masked=True,)
            strm_mask        = rioxarray.open_rasterio(filename=fname_strm_mask,masked=True)
            perc_inund_strms = perc_inundation.where(strm_mask==1,numpy.nan)
            strms_p = xarray.where(abs(perc_inund_strms-100.)<atol,1,numpy.nan)
            strms_p.rio.to_raster(fname_p)
            if verbose: print(f' writing {fname_np}')
            strms_np         = perc_inund_strms.where((strm_mask==1)&(strms_p.isnull())&(perc_inund_strms>0.),numpy.nan)
            strms_np.rio.to_raster(fname_np)
    if fname_verbose is not None:
        sys.stdout = sys.__stdout__
        sys.stderr = sys.__stderr__
        f.close()

def calculate_summary_perc_inundated(**kwargs):
    dt_start           = kwargs.get('dt_start',                 None)
    dt_end             = kwargs.get('dt_end',                   None)
    inundation_raw_dir = kwargs.get('inundation_raw_dir',       None)
    inundation_sum_dir = kwargs.get('inundation_summary_dir',   None)
    fname_dem          = kwargs.get('fname_dem',                None)
    verbose            = kwargs.get('verbose',                  False)
    overwrite          = kwargs.get('overwrite',                False)
    fname_verbose      = kwargs.get('fname_verbose',         None)
    chunks             = kwargs.get('chunks',                   None)
    if fname_verbose is not None:
        f = open(fname_verbose, "a", buffering=1)
        sys.stdout = f
        sys.stderr = f
    if verbose: print(f'calling calculate_summary_perc_inundated')
    os.makedirs(inundation_sum_dir, exist_ok=True)
    fname_output = os.path.join(inundation_sum_dir,
                                f'percent_inundated_grid_{dt_start.strftime('%Y%m%d')}_to_{dt_end.strftime('%Y%m%d')}.tiff')
    if not os.path.isfile(fname_output) or overwrite:
        if verbose: print(f' writing summary percent inundation grid {fname_output}')
        if chunks is None:
            sumgrid = rioxarray.open_rasterio(filename=fname_dem,masked=True).sel(band=1)
            sumgrid.load()
            sumgrid.close()
            sumgrid = sumgrid.where(sumgrid.isnull(),0.)
            idt     = dt_start
            while idt < dt_end:
                dt_str          = idt.strftime('%Y%m%d')
                fname_inund_dti = os.path.join(inundation_raw_dir,f'inundation_{dt_str}.tiff')
                with rioxarray.open_rasterio(filename=fname_inund_dti,masked=True) as riox_inun_dti:
                    inun_dti = riox_inun_dti.sel(band=1).load()
                    inun_dti = inun_dti.where(~inun_dti.isnull(),0)
                    sumgrid  = sumgrid + inun_dti
                idt += datetime.timedelta(days=1)
            perc_inun = (sumgrid/float((dt_end-dt_start).days))*100.
            perc_inun = perc_inun.where(perc_inun>0.,numpy.nan)
            perc_inun.rio.to_raster(fname_output)
        else:
            idt = dt_start
            flist = list()
            while idt < dt_end:
                dt_str = idt.strftime('%Y%m%d')
                fname_inund_dti = os.path.join(inundation_raw_dir,f'inundation_{dt_str}.tiff')
                flist.append(fname_inund_dti)
                idt += datetime.timedelta(days=1)
            dsets = [rioxarray.open_rasterio(f, masked=True,chunks=chunks) for f in flist]
            for i in range(len(dsets)):
                dsets[i] = dsets[i].fillna(0).astype(numpy.uint8)
            stacked = xarray.concat(dsets, dim="concat_dim")
            total_sum = stacked.sum(dim="concat_dim", skipna=True).astype(numpy.float64)
            perc_inun = (total_sum/float((dt_end-dt_start).days))*100.
            perc_inun = perc_inun.where(perc_inun>0.,numpy.nan)
            perc_inun.rio.write_nodata(numpy.nan, inplace=True)
            perc_inun.rio.to_raster(fname_output)
    else:
        if verbose: print(f' found existing summary percent inundated grid {fname_output}')
    if fname_verbose is not None:
        sys.stdout = sys.__stdout__
        sys.stderr = sys.__stderr__
        f.close()
    return fname_output

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
    fname_verbose      = kwargs.get('fname_verbose',            None)
    chunks             = kwargs.get('chunks',                   None)
    if fname_verbose is not None:
        f = open(fname_verbose, "a", buffering=1)
        sys.stdout = f
        sys.stderr = f
    if verbose: print(f'calling calculate_inundation')
    if overwrite: calc_flag = True
    else:         calc_flag = _check_exist(inundation_out_dir,dt_start,dt_end)
    if calc_flag:
        if verbose: print(f' writing output to {inundation_out_dir}')
        os.makedirs(inundation_out_dir, exist_ok=True)
        if chunks is not None:
            twi         = rioxarray.open_rasterio(filename=fname_twi,masked=True,chunks=chunks)
            twi_mean    = rioxarray.open_rasterio(filename=fname_twi_mean,masked=True,chunks=chunks)
            soil_transm = rioxarray.open_rasterio(filename=fname_soil_trans,masked=True,chunks=chunks)
        else:
            twi         = rioxarray.open_rasterio(filename=fname_twi,masked=True)
            twi_mean    = rioxarray.open_rasterio(filename=fname_twi_mean,masked=True)
            soil_transm = rioxarray.open_rasterio(filename=fname_soil_trans,masked=True)
        idt         = dt_start
        while idt <= dt_end:
            dt_str             = idt.strftime('%Y%m%d')
            fname_wtd_mean_raw = os.path.join(wtd_raw_dir,f'wtd_{dt_str}.tiff')
            fname_inund        = os.path.join(inundation_out_dir,f'inundation_{dt_str}.tiff')
            if not os.path.isfile(fname_wtd_mean_raw):
                raise Exception(f'calculate_inundation could not find {fname_wtd_mean_raw}')
            if not os.path.isfile(fname_inund) or overwrite:
                with rioxarray.open_rasterio(fname_wtd_mean_raw,masked=True) as rioxds_wtd_raw:
                    rioxds_wtd_resampled = rioxds_wtd_raw.rio.reproject_match(twi,resampling=Resampling.bilinear)
                if chunks is not None:
                    rioxds_wtd_resampled = rioxds_wtd_resampled.chunk({'band':1, 'x': 1000, 'y': 1000})
                    inun_local = xarray.apply_ufunc(_calc_inundation_time_i,
                                                    rioxds_wtd_resampled, twi, twi_mean, soil_transm,
                                                    dask="parallelized",     
                                                    output_dtypes=numpy.float64,
                                                    keep_attrs=True)
                else:
                    inun_local = xarray.apply_ufunc(_calc_inundation_time_i,
                                                    rioxds_wtd_resampled, twi, twi_mean, soil_transm,
                                                    dask="forbidden",     
                                                    output_dtypes=numpy.float64,
                                                    keep_attrs=True)
                inun_local.rio.write_crs(twi.rio.crs, inplace=True)
                inun_local = inun_local.fillna(0)
                inun_local = inun_local.astype(numpy.uint8)
                inun_local.rio.write_nodata(0, inplace=True)
                inun_local.rio.to_raster(fname_inund)
                if wtd_resampled_dir is not None:
                    os.makedirs(wtd_resampled_dir, exist_ok=True)
                    fname_wtd_mean_warped = os.path.join(wtd_resampled_dir,f'wtd_{dt_str}.tiff')
                    rioxds_wtd_resampled.rio.to_raster(fname_wtd_mean_warped)
            idt += datetime.timedelta(days=1)
    else:
        if verbose: print(f' found existing inundation calculations in {inundation_out_dir}')
    if fname_verbose is not None:
        sys.stdout = sys.__stdout__
        sys.stderr = sys.__stderr__
        f.close()

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