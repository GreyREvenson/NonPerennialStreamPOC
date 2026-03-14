import os
import sys
import rasterio
import py3dep
import rioxarray
from geocube.api.core import make_geocube
from whitebox.whitebox_tools import WhiteboxTools as wbt

async def download_dem(**kwargs):
    domain        = kwargs.get('domain',    None)
    verbose       = kwargs.get('verbose',   False)
    overwrite     = kwargs.get('overwrite', False)
    dem_rez       = kwargs.get('dem_rez',   None)
    fname_dem     = kwargs.get('fname_dem', None)
    fname_verbose = kwargs.get('fname_verbose', None)
    if fname_verbose is not None:
        f = open(fname_verbose, "a", buffering=1)
        sys.stdout = f
        sys.stderr = f
    if not os.path.isfile(fname_dem) or overwrite:
        avail = py3dep.check_3dep_availability(bbox=tuple(domain.total_bounds),crs=domain.crs)
        vals  = list()
        for k in avail.keys():
            try: vals.append(int(k.replace('m','')))
            except: pass
        if len(vals) == 0:
            raise ValueError(f'download_dem 3dep could not find dem resolution')
        rez = min(vals)
        if dem_rez in vals: rez = dem_rez # override with user input if available
        if verbose:
            print(f' downloading dem ({str(rez)}m) using py3dep. saving to: {fname_dem}')
        dem = py3dep.get_dem(geometry   = domain.geometry.union_all(),
                             resolution = rez,
                             crs        = domain.crs)
        dem.rio.to_raster(fname_dem)
    else:
        if verbose: print(f' using existing dem {fname_dem}')
    if fname_verbose is not None:
        sys.stdout = sys.__stdout__
        sys.stderr = sys.__stderr__
        f.close()

def set_domain_mask(**kwargs):
    domain            = kwargs.get('domain',            None)
    verbose           = kwargs.get('verbose',           False)
    overwrite         = kwargs.get('overwrite',         False)
    fname_domain_mask = kwargs.get('fname_domain_mask', None)
    fname_dem         = kwargs.get('fname_dem',         None)
    fname_verbose     = kwargs.get('fname_verbose',         None)
    if fname_verbose is not None:
        f = open(fname_verbose, "a", buffering=1)
        sys.stdout = f
        sys.stderr = f
    if verbose: print(f'calling set_domain_mask')
    if not os.path.isfile(fname_dem):
        raise ValueError(f'set_domain_mask could not find fname_dem {fname_dem}')
    if not os.path.isfile(fname_domain_mask) or overwrite:
        if verbose: print(f' creating {fname_domain_mask}')
        with rioxarray.open_rasterio(fname_dem) as riox_ds_dem:
            domain = domain.to_crs(riox_ds_dem.rio.crs)
            domain = domain.dissolve()
            domain = domain.drop(columns=[col for col in domain.columns if col not in ['geometry']])
            domain['mask'] = 1
            domain_mask = make_geocube(vector_data=domain,like=riox_ds_dem,measurements=['mask'])
            domain_mask.rio.to_raster(fname_domain_mask)
    else:
        if verbose: print(f' using existing domain mask {fname_domain_mask}')
    if fname_verbose is not None:
        sys.stdout = sys.__stdout__
        sys.stderr = sys.__stderr__
        f.close()

def breach_dem(**kwargs):
    fname_dem_breached = kwargs.get('fname_dem_breached', None)
    fname_dem          = kwargs.get('fname_dem',          None)
    verbose            = kwargs.get('verbose',            False)
    overwrite          = kwargs.get('overwrite',          False)
    fname_verbose      = kwargs.get('fname_verbose',         None)
    if verbose: print('calling breach_dem')
    if fname_dem is None:
        raise KeyError('breach_dem missing required argument fname_dem')
    if not os.path.isfile(fname_dem):
        raise ValueError(f'breach_dem could not find fname_dem {fname_dem}')
    if not os.path.isfile(fname_dem_breached) or overwrite:
        if verbose: print(f' using whitebox to breach dem and writing to {fname_dem_breached}')
        wbt.breach_depressions_least_cost(
            dem=fname_dem,
            output=fname_dem_breached,
            max_dist=1000
        )
    else:
        if verbose: print(f' using existing breached dem {fname_dem_breached}')

def set_flow_acc(**kwargs):
    fname_dem_breached = kwargs.get('fname_dem_breached', None)
    fname_facc_ncells  = kwargs.get('fname_facc_ncells',  None)
    fname_facc_sca     = kwargs.get('fname_facc_sca',     None)
    verbose            = kwargs.get('verbose',            False)
    overwrite          = kwargs.get('overwrite',          False)
    fname_verbose      = kwargs.get('fname_verbose',         None)
    if verbose: print(f'calling set_flow_acc')
    if not os.path.isfile(fname_facc_ncells) or overwrite:
        if verbose: print(f' using whitebox to calculate flow accumulation (n cells), writing to {fname_facc_ncells}')
        wbt.d_inf_flow_accumulation(i        = fname_dem_breached,
                                    output   = fname_facc_ncells,
                                    out_type = 'cells',
                                    log      = False)
    else:
        if verbose: print(f' using existing flow accumulation (ncells) file {fname_facc_ncells}')
    if not os.path.isfile(fname_facc_sca) or overwrite:
        if verbose: print(f' using whitebox to calculate flow accumulation (sca), writing to {fname_facc_sca}')
        wbt.d_inf_flow_accumulation(i        = fname_dem_breached,
                                    output   = fname_facc_sca,
                                    out_type = 'sca',
                                    log      = False)

    else:
        if verbose: print(f' using existing flow accumulation (sca) file {fname_facc_sca}')

def calc_stream_mask(**kwargs):
    ##TODO fnames might not be os.path.isfile()
    verbose               = kwargs.get('verbose',              False)
    overwrite             = kwargs.get('overwrite',            False)
    fname_facc_ncells     = kwargs.get('fname_facc_ncells',    None)
    fname_facc_sca        = kwargs.get('fname_facc_sca',       None)
    facc_threshold_ncells = kwargs.get('facc_threshold_ncells',None)
    facc_threshold_sca    = kwargs.get('facc_threshold_sca',   None)
    fname_strm_mask       = kwargs.get('fname_strm_mask',      None)
    fname_verbose         = kwargs.get('fname_verbose',         None)
    if verbose: print('calling calc_stream_mask')
    if not os.path.isfile(fname_strm_mask) or overwrite:
        if os.path.isfile(fname_facc_ncells):
            if verbose: print(f' setting stream mask using fname_facc_ncells {fname_facc_ncells} and facc_threshold_ncells {facc_threshold_ncells}')
            wbt.extract_streams(flow_accum      = fname_facc_ncells,
                                output          = fname_strm_mask,
                                threshold       = facc_threshold_ncells,
                                zero_background = True)
        elif os.path.isfile(fname_facc_sca):
            if verbose: print(f' setting stream mask using fname_facc_sca {fname_facc_sca} and facc_threshold_sca {facc_threshold_sca}')
            wbt.extract_streams(flow_accum      = fname_facc_sca,
                                output          = fname_strm_mask,
                                threshold       = facc_threshold_sca,
                                zero_background = True)
        else:
            raise Exception(f'calc_stream_mask did not find valid flow accumulation file fname_facc_ncells {fname_facc_ncells} or fname_facc_sca {fname_facc_sca}')
    else:
        if verbose: print(f' using existing stream mask {fname_strm_mask}')

def calc_slope(**kwargs):
    fname_dem_breached = kwargs.get('fname_dem_breached', None)
    fname_slope        = kwargs.get('fname_slope',        None)
    verbose            = kwargs.get('verbose',            False)
    overwrite          = kwargs.get('overwrite',          False)
    fname_verbose      = kwargs.get('fname_verbose',         None)
    if verbose: print(f'calling calc_slope')
    if not os.path.isfile(fname_slope) or overwrite:
        wbt.slope(dem    = fname_dem_breached,
                  output = fname_slope,
                  units  = 'degrees')
    else:
        if verbose: print(f' using existing slope file {fname_slope}')

def calc_twi(**kwargs):
    fname_facc_sca     = kwargs.get('fname_facc_sca', None)
    fname_slope        = kwargs.get('fname_slope',    None)
    fname_twi          = kwargs.get('fname_twi',      None)
    verbose            = kwargs.get('verbose',        False)
    overwrite          = kwargs.get('overwrite',      False)
    fname_verbose      = kwargs.get('fname_verbose',         None)
    if verbose: print('calling calc_twi')
    if not os.path.isfile(fname_twi) or overwrite:
        if verbose: print(f' using whitebox workflows to calculate {fname_twi}')
        wbt.wetness_index(sca    = fname_facc_sca,
                          slope  = fname_slope,
                          output = fname_twi)
    else:
        if verbose: print(f' found existing TWI file {fname_twi}')

def calc_twi_mean(**kwargs):
    fname_twi          = kwargs.get('fname_twi',      None)
    fname_twi_mean     = kwargs.get('fname_twi_mean', None)
    wtd_raw_dir        = kwargs.get('wtd_raw_dir',    None)
    verbose            = kwargs.get('verbose',        False)
    overwrite          = kwargs.get('overwrite',      False)
    fname_verbose      = kwargs.get('fname_verbose',         None)
    if verbose: print('calling calc_twi_mean')
    if not os.path.isfile(fname_twi_mean) or overwrite:
        fname_example_wtd_raw = [fn for fn in os.listdir(wtd_raw_dir) if str(fn).endswith('.tiff')][0]
        fname_example_wtd_raw = os.path.join(wtd_raw_dir,fname_example_wtd_raw)
        if not os.path.isfile(fname_example_wtd_raw):
            raise ValueError(f'calc_twi_mean could not locate example raw wtd file - {fname_example_wtd_raw} is not valid file')
        if verbose: print(f' calculating mean twi and saving to {fname_twi_mean} (calculating mean twi in each wtd grid cell)')
        with rioxarray.open_rasterio(fname_twi,masked=True) as riox_ds_twi, rioxarray.open_rasterio(fname_example_wtd_raw,masked=True) as riox_ds_wtd:
            twi_mean = riox_ds_twi.rio.reproject_match(riox_ds_wtd, resampling=rasterio.enums.Resampling.average)
            twi_mean = twi_mean.rio.reproject_match(riox_ds_twi, resample=rasterio.enums.Resampling.nearest)
            twi_mean.rio.to_raster(fname_twi_mean,compress=True)
    else:
        if verbose: print(f' found existing mean TWI file {fname_twi_mean}')






